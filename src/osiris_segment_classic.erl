-module(osiris_segment_classic).

-include("osiris.hrl").
-include("osiris_log.hrl").
-include_lib("kernel/include/file.hrl").

-export([init/1,
         init/2,
         chunk_iterator/1,
         chunk_iterator/2,
         iterator_next/1,
         read_header/1]).

-export_type([state/0,
              chunk_iterator/0
]).


-define(REC_MATCH_SIMPLE(Len, Rem),
        <<0:1, Len:31/unsigned, Rem/binary>>).
-define(REC_MATCH_SUBBATCH(CompType, NumRec, UncompLen, Len, Rem),
        <<1:1, CompType:3/unsigned, _:4/unsigned,
          NumRecs:16/unsigned,
          UncompressedLen:32/unsigned,
          Len:32/unsigned, Rem/binary>>).

-define(REC_HDR_SZ_SIMPLE_B, 4).
-define(REC_HDR_SZ_SUBBATCH_B, 11).
-define(ITER_READ_AHEAD_B, 64).

-type config() ::
    osiris:config() |
    #{dir := file:filename_all(),
      epoch => non_neg_integer(),
      % first_offset_fun => fun((integer()) -> ok),
      shared => atomics:atomics_ref(),
      max_segment_size_bytes => non_neg_integer(),
      %% max number of writer ids to keep around
      tracking_config => osiris_tracking:config(),
      %% if the counter is created before init is passed here
      counter => counters:counters_ref(),
      %% spec for creating the counter
      counter_spec => counter_spec(),
      %% used when initialising a log from an offset other than 0
      initial_offset => osiris:offset(),
      %% a cached list of the index files for a given log
      %% avoids scanning disk for files multiple times if already know
      %% e.g. in init_acceptor
      index_files => [file:filename_all()],
      filter_size => osiris_bloom:filter_size()
     }.

%% TODO fix later, just to make tests pass. Need to up date the init method
-record(osiris_log,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename_all(),
         index_fd :: undefined | file:io_device(),
         fd :: undefined | file:io_device()
        }).

%% record chunk_info does not map exactly to an index record (field 'num' differs)
-record(chunk_info,
        {id :: offset(),
         timestamp :: non_neg_integer(),
         epoch :: epoch(),
         num :: non_neg_integer(),
         type :: chunk_type(),
         %% size of data + filter + trailer
         size :: non_neg_integer(),
         %% position in segment file
         pos :: integer()
        }).
-record(seg_info,
        {file :: file:filename_all(),
         size = 0 :: non_neg_integer(),
         index :: file:filename_all(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).

-define(MODULE_TMP, osiris_log).

-opaque state() :: #osiris_log{}.

%% -type counter_spec() :: {Tag :: term(), Fields :: [atom()]}.
%% -type chunk_type() ::
%%     ?CHNK_USER |
%%     ?CHNK_TRK_DELTA |
%%     ?CHNK_TRK_SNAPSHOT.
%% -type config() ::
%%     osiris:config() |
%%     #{dir := file:filename_all(),
%%       epoch => non_neg_integer(),
%%       % first_offset_fun => fun((integer()) -> ok),
%%       shared => atomics:atomics_ref(),
%%       max_segment_size_bytes => non_neg_integer(),
%%       %% max number of writer ids to keep around
%%       tracking_config => osiris_tracking:config(),
%%       %% if the counter is created before init is passed here
%%       counter => counters:counters_ref(),
%%       %% spec for creating the counter
%%       counter_spec => counter_spec(),
%%       %% used when initialising a log from an offset other than 0
%%       initial_offset => osiris:offset(),
%%       %% a cached list of the index files for a given log
%%       %% avoids scanning disk for files multiple times if already know
%%       %% e.g. in init_acceptor
%%       index_files => [file:filename_all()],
%%       filter_size => osiris_bloom:filter_size()
%%      }.
%% -type record() :: {offset(), osiris:entry()}.
-type offset_entry() :: {offset(), osiris:entry()}.
%% -type offset_spec() :: osiris:offset_spec().
%% -type retention_spec() :: osiris:retention_spec().

-spec init(config()) -> state().
init(Config) ->
    init(Config, writer).

-spec init(config(), writer | acceptor) -> state().
init(#{dir := Dir,
       name := Name,
       epoch := Epoch} = Config,
     WriterType) ->
    %% scan directory for segments if in write mode
    MaxSizeBytes = maps:get(max_segment_size_bytes, Config,
                            ?DEFAULT_MAX_SEGMENT_SIZE_B),
    MaxSizeChunks = application:get_env(osiris, max_segment_size_chunks,
                                        ?DEFAULT_MAX_SEGMENT_SIZE_C),
    Retention = maps:get(retention, Config, []),
    FilterSize = maps:get(filter_size, Config, ?DEFAULT_FILTER_SIZE),
    ?INFO("Stream: ~ts will use ~ts for osiris log data directory",
          [Name, Dir]),
    ?DEBUG_(Name, "max_segment_size_bytes: ~b,
           max_segment_size_chunks ~b, retention ~w, filter size ~b",
            [MaxSizeBytes, MaxSizeChunks, Retention, FilterSize]),
    ok = filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        Err ->
            throw(Err)
    end,

    Cnt = osiris_log:make_counter(Config),
    %% initialise offset counter to -1 as 0 is the first offset in the log and
    %% it hasn't necessarily been written yet, for an empty log the first offset
    %% is initialised to 0 however and will be updated after each retention run.
    counters:put(Cnt, ?C_OFFSET, -1),
    counters:put(Cnt, ?C_SEGMENTS, 0),
    Shared = case Config of
                 #{shared := S} ->
                     S;
                 _ ->
                     osiris_log_shared:new()
             end,
    Cfg = #cfg{directory = Dir,
               name = Name,
               max_segment_size_bytes = MaxSizeBytes,
               max_segment_size_chunks = MaxSizeChunks,
               tracking_config = maps:get(tracking_config, Config, #{}),
               retention = Retention,
               counter = Cnt,
               counter_id = osiris_log:counter_id(Config),
               shared = Shared,
               filter_size = FilterSize},
    ok = maybe_fix_corrupted_files(Config),
    DefaultNextOffset = case Config of
                            #{initial_offset := IO}
                              when WriterType == acceptor ->
                                IO;
                            _ ->
                                0
                        end,
    case first_and_last_seginfos(Config) of
        none ->
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            open_new_segment(#?MODULE_TMP{cfg = Cfg,
                                      mode =
                                          #write{type = WriterType,
                                                 tail_info = {DefaultNextOffset,
                                                              empty},
                                                 current_epoch = Epoch}});
        {NumSegments,
         #seg_info{first = #chunk_info{id = FstChId,
                                       timestamp = FstTs}},
         #seg_info{file = Filename,
                   index = IdxFilename,
                   size = Size,
                   last = #chunk_info{epoch = LastEpoch,
                                      timestamp = LastTs,
                                      id = LastChId,
                                      num = LastNum}}} ->
            %% assert epoch is same or larger
            %% than last known epoch
            case LastEpoch > Epoch of
                true ->
                    exit({invalid_epoch, LastEpoch, Epoch});
                _ ->
                    ok
            end,
            TailInfo = {LastChId + LastNum,
                        {LastEpoch, LastChId, LastTs}},

            counters:put(Cnt, ?C_FIRST_OFFSET, FstChId),
            counters:put(Cnt, ?C_FIRST_TIMESTAMP, FstTs),
            counters:put(Cnt, ?C_OFFSET, LastChId + LastNum - 1),
            counters:put(Cnt, ?C_SEGMENTS, NumSegments),
            osiris_log_shared:set_first_chunk_id(Shared, FstChId),
            osiris_log_shared:set_last_chunk_id(Shared, LastChId),
            ?DEBUG_(Name, " next offset ~b first offset ~b",
                    [element(1, TailInfo),
                     FstChId]),
            {ok, SegFd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, Size} = file:position(SegFd, Size),
            %% maybe_fix_corrupted_files has truncated the index to the last
            %% record pointing
            %% at a valid chunk we can now truncate the segment to size in
            %% case there is trailing data
            ok = file:truncate(SegFd),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            {ok, IdxEof} = file:position(IdxFd, eof),
            NumChunks = (IdxEof - ?IDX_HEADER_SIZE) div ?INDEX_RECORD_SIZE_B,
            #?MODULE_TMP{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = TailInfo,
                                segment_size = {Size, NumChunks},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     fd = SegFd,
                     index_fd = IdxFd};
        {1, #seg_info{file = Filename,
                      index = IdxFilename,
                      last = undefined}, _} ->
            %% the empty log case
            {ok, SegFd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            {ok, _} = file:position(SegFd, ?LOG_HEADER_SIZE),
            counters:put(Cnt, ?C_SEGMENTS, 1),
            %% the segment could potentially have trailing data here so we'll
            %% do a truncate just in case. The index would have been truncated
            %% earlier
            ok = file:truncate(SegFd),
            {ok, _} = file:position(IdxFd, ?IDX_HEADER_SIZE),
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            #?MODULE_TMP{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = {DefaultNextOffset, empty},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     fd = SegFd,
                     index_fd = IdxFd}
    end.


maybe_fix_corrupted_files([]) ->
    ok;
maybe_fix_corrupted_files(#{dir := Dir}) ->
    ok = maybe_fix_corrupted_files(sorted_index_files(Dir)),
    %% dangling segments can be left behind if the server process crashes
    %% after the retention evaluator process deleted the index but
    %% before it deleted the corresponding segment
    [begin
         ?INFO("deleting left over segment '~s' in directory ~s",
               [F, Dir]),
         ok = prim_file:delete(filename:join(Dir, F))
     end|| F <- orphaned_segments(Dir)],
    ok;
maybe_fix_corrupted_files([IdxFile]) ->
    SegFile = segment_from_index_file(IdxFile),
    ok = truncate_invalid_idx_records(IdxFile, file_size_or_zero(SegFile)),
    case file_size(IdxFile) =< ?IDX_HEADER_SIZE + ?INDEX_RECORD_SIZE_B of
        true ->
            % the only index doesn't contain a single valid record
            % make sure it has a valid header
            {ok, IdxFd} = file:open(IdxFile, ?FILE_OPTS_WRITE),
            ok = file:write(IdxFd, ?IDX_HEADER),
            ok = file:close(IdxFd);
        false ->
            ok
    end,
    case file_size_or_zero(SegFile) =< ?LOG_HEADER_SIZE + ?HEADER_SIZE_B of
        true ->
            % the only segment doesn't contain a single valid chunk
            % make sure it has a valid header
            {ok, SegFd} = file:open(SegFile, ?FILE_OPTS_WRITE),
            ok = file:write(SegFd, ?LOG_HEADER),
            ok = file:close(SegFd);
        false ->
            ok
    end;
maybe_fix_corrupted_files(IdxFiles) ->
    LastIdxFile = lists:last(IdxFiles),
    LastSegFile = segment_from_index_file(LastIdxFile),
    try file_size(LastSegFile) of
        N when N =< ?HEADER_SIZE_B ->
            % if the segment doesn't contain any chunks, just delete it
            ?WARNING("deleting an empty segment file: ~0p", [LastSegFile]),
            ok = prim_file:delete(LastIdxFile),
            ok = prim_file:delete(LastSegFile),
            maybe_fix_corrupted_files(IdxFiles -- [LastIdxFile]);
        LastSegFileSize ->
            ok = truncate_invalid_idx_records(LastIdxFile, LastSegFileSize)
    catch missing_file ->
            % if the last segment is missing, just delete its index
            ?WARNING("deleting index of the missing last segment file: ~0p",
                     [LastSegFile]),
            ok = prim_file:delete(LastIdxFile),
            maybe_fix_corrupted_files(IdxFiles -- [LastIdxFile])
    end.


sorted_index_files(#{index_files := IdxFiles}) ->
    %% cached
    IdxFiles;
sorted_index_files(#{dir := Dir}) ->
    sorted_index_files(Dir);
sorted_index_files(Dir) when ?IS_STRING(Dir) ->
    index_files(Dir, fun lists:sort/1).

%% sorted_index_files_rev(#{index_files := IdxFiles}) ->
%%     %% cached
%%     lists:reverse(IdxFiles);
%% sorted_index_files_rev(#{dir := Dir}) ->
%%     sorted_index_files_rev(Dir);
%% sorted_index_files_rev(Dir) ->
%%     index_files(Dir, fun (Files) ->
%%                              lists:sort(fun erlang:'>'/2, Files)
%%                      end).

%% index_files_unsorted(Dir) ->
%%     index_files(Dir, fun (X) -> X end).

index_files(Dir, SortFun) ->
    [filename:join(Dir, F)
     || <<_:20/binary, ".index">> = F <- SortFun(list_dir(Dir))].

orphaned_segments(Dir) ->
    orphaned_segments(lists:sort(list_dir(Dir)), []).

orphaned_segments([], Acc) ->
    Acc;
orphaned_segments([<<_:20/binary, ".index">>], Acc) ->
    Acc;
orphaned_segments([<<Name:20/binary, ".index">>,
                   <<Name:20/binary, ".segment">> | _Rem],
                  Acc) ->
    %% when we find a matching pair we can return
    Acc;
orphaned_segments([<<_:20/binary, ".segment">> = Dangler | Rem], Acc) ->
    orphaned_segments(Rem, [Dangler | Acc]);
orphaned_segments([_Unexpected | Rem], Acc) ->
    %% just ignore unexpected files
    orphaned_segments(Rem, Acc).

first_and_last_seginfos(#{index_files := IdxFiles}) ->
    first_and_last_seginfos0(IdxFiles);
first_and_last_seginfos(#{dir := Dir}) ->
    first_and_last_seginfos0(sorted_index_files(Dir)).

first_and_last_seginfos0([]) ->
    none;
first_and_last_seginfos0([FstIdxFile]) ->
    {ok, SegInfo} = build_seg_info(FstIdxFile),
    {1, SegInfo, SegInfo};
first_and_last_seginfos0([FstIdxFile | Rem] = IdxFiles) ->
    %% this function is only used by init
    case build_seg_info(FstIdxFile) of
        {ok, FstSegInfo} ->
            LastIdxFile = lists:last(Rem),
            case build_seg_info(LastIdxFile) of
                {ok, #seg_info{first = undefined,
                               last = undefined}} ->
                    %% the last index file doesn't have any index records yet
                    %% retry without it
                    [_ | RetryIndexFiles] = lists:reverse(IdxFiles),
                    first_and_last_seginfos0(lists:reverse(RetryIndexFiles));
                {ok, LastSegInfo} ->
                    {length(Rem) + 1, FstSegInfo, LastSegInfo};
                {error, Err} ->
                    ?ERROR("~s: failed to build seg_info from file ~ts, error: ~w",
                           [?MODULE_TMP, LastIdxFile, Err]),
                    error(Err)
            end;
        {error, enoent} ->
            %% most likely retention race condition
            first_and_last_seginfos0(Rem)
    end.

build_seg_info(IdxFile) ->
    case last_valid_idx_record(IdxFile) of
        {ok, ?IDX_MATCH(_, _, LastChunkPos)} ->
            SegFile = segment_from_index_file(IdxFile),
            build_segment_info(SegFile, LastChunkPos, IdxFile);
        undefined ->
            %% this would happen if the file only contained a header
            SegFile = segment_from_index_file(IdxFile),
            {ok, #seg_info{file = SegFile, index = IdxFile}};
        {error, _} = Err ->
            Err
    end.

%% last_idx_record(IdxFd) ->
%%     nth_last_idx_record(IdxFd, 1).

nth_last_idx_record(IdxFile, N) when ?IS_STRING(IdxFile) ->
    {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
    IdxRecord = nth_last_idx_record(IdxFd, N),
    _ = file:close(IdxFd),
    IdxRecord;
nth_last_idx_record(IdxFd, N) ->
    case position_at_idx_record_boundary(IdxFd, {eof, -?INDEX_RECORD_SIZE_B * N}) of
        {ok, _} ->
            file:read(IdxFd, ?INDEX_RECORD_SIZE_B);
        Err ->
            Err
    end.

last_valid_idx_record(IdxFile) ->
    {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
    case position_at_idx_record_boundary(IdxFd, eof) of
        {ok, Pos} ->
            SegFile = segment_from_index_file(IdxFile),
            SegSize = file_size(SegFile),
            ok = skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos),
            case file:position(IdxFd, {cur, -?INDEX_RECORD_SIZE_B}) of
                {ok, _} ->
                    IdxRecord = file:read(IdxFd, ?INDEX_RECORD_SIZE_B),
                    _ = file:close(IdxFd),
                    IdxRecord;
                _ ->
                    _ = file:close(IdxFd),
                    undefined
            end;
        Err ->
            _ = file:close(IdxFd),
            Err
    end.

%% first_idx_record(IdxFd) ->
%%     idx_read_at(IdxFd, ?IDX_HEADER_SIZE).

%% idx_read_at(Fd, Pos) when is_integer(Pos) ->
%%     case file:pread(Fd, Pos, ?INDEX_RECORD_SIZE_B) of
%%         {ok, ?ZERO_IDX_MATCH(_)} ->
%%             {error, empty_idx_record};
%%         Ret ->
%%             Ret
%%     end.

%% Some file:position/2 operations are subject to race conditions. In particular, `eof` may position the Fd
%% in the middle of a record being written concurrently. If that happens, we need to re-position at the nearest
%% record boundry. See https://github.com/rabbitmq/osiris/issues/73
position_at_idx_record_boundary(IdxFd, At) ->
    case file:position(IdxFd, At) of
        {ok, Pos} ->
            case (Pos - ?IDX_HEADER_SIZE) rem ?INDEX_RECORD_SIZE_B of
                0 -> {ok, Pos};
                N -> file:position(IdxFd, {cur, -N})
            end;
        Error -> Error
    end.

build_segment_info(SegFile, LastChunkPos, IdxFile) ->
    {ok, Fd} = open(SegFile, [read, binary, raw]),
    %% we don't want to read blocks into page cache we are unlikely to need
    _ = file:advise(Fd, 0, 0, random),
    case file:pread(Fd, ?LOG_HEADER_SIZE, ?HEADER_SIZE_B) of
        eof ->
            _ = file:close(Fd),
            eof;
        {ok,
         <<?MAGIC:4/unsigned,
           ?VERSION:4/unsigned,
           FirstChType:8/unsigned,
           _NumEntries:16/unsigned,
           FirstNumRecords:32/unsigned,
           FirstTs:64/signed,
           FirstEpoch:64/unsigned,
           FirstChId:64/unsigned,
           _FirstCrc:32/integer,
           FirstSize:32/unsigned,
           FirstFSize:8/unsigned,
           FirstTSize:24/unsigned,
           _/binary>>} ->
            case file:pread(Fd, LastChunkPos, ?HEADER_SIZE_B) of
                {ok,
                 <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   LastChType:8/unsigned,
                   _LastNumEntries:16/unsigned,
                   LastNumRecords:32/unsigned,
                   LastTs:64/signed,
                   LastEpoch:64/unsigned,
                   LastChId:64/unsigned,
                   _LastCrc:32/integer,
                   LastSize:32/unsigned,
                   LastTSize:32/unsigned,
                   LastFSize:8/unsigned,
                   _Reserved:24>>} ->
                    LastChunkSize = LastFSize + LastSize + LastTSize,
                    Size = LastChunkPos + ?HEADER_SIZE_B + LastChunkSize,
                    %% TODO: this file:position/2 all has no actual function and
                    %% is only used to emit a debug log. Remove?
                    {ok, Eof} = file:position(Fd, eof),
                    ?DEBUG_IF("~s: segment ~ts has trailing data ~w ~w",
                              [?MODULE_TMP, filename:basename(SegFile),
                               Size, Eof], Size =/= Eof),
                    _ = file:close(Fd),
                    FstChInfo = #chunk_info{epoch = FirstEpoch,
                                            timestamp = FirstTs,
                                            id = FirstChId,
                                            num = FirstNumRecords,
                                            type = FirstChType,
                                            size = FirstFSize + FirstSize + FirstTSize,
                                            pos = ?LOG_HEADER_SIZE},
                    LastChInfo = #chunk_info{epoch = LastEpoch,
                                             timestamp = LastTs,
                                             id = LastChId,
                                             num = LastNumRecords,
                                             type = LastChType,
                                             size = LastChunkSize,
                                             pos = LastChunkPos},
                    {ok, #seg_info{file = SegFile,
                                   index = IdxFile,
                                   size = Size,
                                   first = FstChInfo,
                                   last = LastChInfo}};
                _ ->
                    % last chunk is corrupted - try the previous one
                    _ = file:close(Fd),
                    {ok, ?IDX_MATCH(_ChId, _E, PrevChPos)} =
                        nth_last_idx_record(IdxFile, 2),
                    case PrevChPos == LastChunkPos of
                        false ->
                            build_segment_info(SegFile, PrevChPos , IdxFile);
                        true ->
                            % avoid an infinite loop if multiple chunks are corrupted
                            ?ERROR("Multiple corrupted chunks in segment file ~0p",
                                   [SegFile]),
                            exit({corrupted_segment, {segment_file, SegFile}})
                    end
            end
    end.


%% TODO Concider using a map instead of record, to be able to
%% handle the opaque definition in the top module?
-record(iterator, {fd :: file:io_device(),
                   next_offset :: offset(),
                   %% entries left
                   num_left :: non_neg_integer(),
                   %% any trailing data from last read
                   %% we try to capture at least the size of the next record
                   data :: undefined | binary(),
                   next_record_pos :: non_neg_integer()}).

-opaque chunk_iterator() :: #iterator{}.
-spec chunk_iterator(state()) ->
    {ok, header_map(), chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(State) ->
    chunk_iterator(State, 1).

-spec chunk_iterator(state(), pos_integer() | all) ->
    {ok, header_map(), chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(#?MODULE_TMP{cfg = #cfg{},
                        mode = #read{type = RType,
                                     chunk_selector = Selector}
                       } = State0, CreditHint)
  when (is_integer(CreditHint) andalso CreditHint > 0) orelse
       is_atom(CreditHint) ->
    %% reads the next chunk of unparsed chunk data
    case catch read_header0(State0) of
        {ok,
         #{type := ChType,
           chunk_id := ChId,
           crc := Crc,
           num_entries := NumEntries,
           num_records := NumRecords,
           data_size := DataSize,
           filter_size := FilterSize,
           position := Pos,
           next_position := NextPos} = Header,
         #?MODULE_TMP{fd = Fd, mode = #read{next_offset = ChId} = Read} = State1} ->
            State = State1#?MODULE_TMP{mode = Read#read{next_offset = ChId + NumRecords,
                                                    position = NextPos}},
            case osiris_log:needs_handling(RType, Selector, ChType) of
                true ->
                    DataPos = Pos + ?HEADER_SIZE_B + FilterSize,
                    Data = iter_read_ahead(Fd, DataPos, ChId, Crc, CreditHint,
                                           DataSize, NumEntries),
                    Iterator = #iterator{fd = Fd,
                                         data = Data,
                                         next_offset = ChId,
                                         num_left = NumEntries,
                                         next_record_pos = DataPos},
                    {ok, Header, Iterator, State};
                false ->
                    %% skip
                    chunk_iterator(State, CreditHint)
            end;
        Other ->
            Other
    end.

-spec iterator_next(osiris_log:chunk_iterator()) ->
    end_of_chunk | {offset_entry(), osiris_log:chunk_iterator()}.
iterator_next(#iterator{num_left = 0}) ->
    end_of_chunk;
iterator_next(#iterator{fd = Fd,
                        next_offset = NextOffs,
                        num_left = Num,
                        data = ?REC_MATCH_SIMPLE(Len, Rem0),
                        next_record_pos = Pos} = I0) ->
    {Record, Rem} =
        case Rem0 of
            <<Record0:Len/binary, Rem1/binary>> ->
                {Record0, Rem1};
            _ ->
                %% not enough in Rem0 to read the entire record
                %% so we need to read it from disk
                {ok, <<Record0:Len/binary, Rem1/binary>>} =
                    file:pread(Fd, Pos + ?REC_HDR_SZ_SIMPLE_B,
                               Len + ?ITER_READ_AHEAD_B),
                {Record0, Rem1}
        end,

    I = I0#iterator{next_offset = NextOffs + 1,
                    num_left = Num - 1,
                    data = Rem,
                    next_record_pos = Pos + ?REC_HDR_SZ_SIMPLE_B + Len},
    {{NextOffs, Record}, I};
iterator_next(#iterator{fd = Fd,
                        next_offset = NextOffs,
                        num_left = Num,
                        data = ?REC_MATCH_SUBBATCH(CompType, NumRecs,
                                                   UncompressedLen,
                                                   Len, Rem0),
                        next_record_pos = Pos} = I0) ->
    {Data, Rem} =
        case Rem0 of
            <<Record0:Len/binary, Rem1/binary>> ->
                {Record0, Rem1};
            _ ->
                %% not enough in Rem0 to read the entire record
                %% so we need to read it from disk
                {ok, <<Record0:Len/binary, Rem1/binary>>} =
                    file:pread(Fd, Pos + ?REC_HDR_SZ_SUBBATCH_B,
                               Len + ?ITER_READ_AHEAD_B),
                {Record0, Rem1}
        end,
    Record = {batch, NumRecs, CompType, UncompressedLen, Data},
    I = I0#iterator{next_offset = NextOffs + NumRecs,
                    num_left = Num - 1,
                    data = Rem,
                    next_record_pos = Pos + ?REC_HDR_SZ_SUBBATCH_B + Len},
    {{NextOffs, Record}, I};
iterator_next(#iterator{fd = Fd,
                        next_record_pos = Pos} = I) ->
    {ok, Data} = file:pread(Fd, Pos, ?ITER_READ_AHEAD_B),
    iterator_next(I#iterator{data = Data}).

iter_read_ahead(_Fd, _Pos, _ChunkId, _Crc, 1, _DataSize, _NumEntries) ->
    %% no point reading ahead if there is only one entry to be read at this
    %% time
    undefined;
iter_read_ahead(Fd, Pos, ChunkId, Crc, Credit, DataSize, NumEntries)
  when Credit == all orelse NumEntries == 1 ->
    {ok, Data} = file:pread(Fd, Pos, DataSize),
    validate_crc(ChunkId, Crc, Data),
    Data;
iter_read_ahead(Fd, Pos, _ChunkId, _Crc, Credit0, DataSize, NumEntries) ->
    %% read ahead, assumes roughly equal entry sizes which may not be the case
    %% TODO round up to nearest block?
    %% We can only practically validate CRC if we read the whole data
    Credit = min(Credit0, NumEntries),
    Size = DataSize div NumEntries * Credit,
    {ok, Data} = file:pread(Fd, Pos, Size + ?ITER_READ_AHEAD_B),
    Data.

validate_crc(ChunkId, Crc, IOData) ->
    case erlang:crc32(IOData) of
        Crc ->
            ok;
        _ ->
            ?ERROR("crc validation failure at chunk id ~bdata size "
                   "~b:",
                   [ChunkId, iolist_size(IOData)]),
            exit({crc_validation_failure, {chunk_id, ChunkId}})
    end.

-spec read_header(state()) ->
    {ok, header_map(), state()} | {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_header(#?MODULE_TMP{cfg = #cfg{}} = State0) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    %% TODO: skip non user chunks for offset readers
    case catch read_header0(State0) of
        {ok,
         #{num_records := NumRecords,
           next_position := NextPos} =
             Header,
         #?MODULE_TMP{mode = #read{next_offset = ChId} = Read} = State} ->
            %% skip data portion
            {ok, Header,
             State#?MODULE_TMP{mode = Read#read{next_offset = ChId + NumRecords,
                                            position = NextPos}}};
        {end_of_stream, _} = EOF ->
            EOF;
        {error, _} = Err ->
            Err
    end.

read_header0(#?MODULE_TMP{cfg = #cfg{directory = Dir,
                                 shared = Shared,
                                 counter = CntRef},
                      mode = #read{next_offset = NextChId0,
                                   position = Pos,
                                   filter = Filter} = Read0,
                      current_file = CurFile,
                      fd = Fd} =
             State) ->
    %% reads the next header if permitted
    case osiris_log:can_read_next(State) of
        true ->
            %% optimistically read 64 bytes (small binary) as it may save us
            %% a syscall reading the filter if the filter is of the default
            %% 16 byte size
            case file:pread(Fd, Pos, ?HEADER_SIZE_B + ?DEFAULT_FILTER_SIZE) of
                {ok, <<?MAGIC:4/unsigned,
                       ?VERSION:4/unsigned,
                       ChType:8/unsigned,
                       NumEntries:16/unsigned,
                       NumRecords:32/unsigned,
                       Timestamp:64/signed,
                       Epoch:64/unsigned,
                       NextChId0:64/unsigned,
                       Crc:32/integer,
                       DataSize:32/unsigned,
                       TrailerSize:32/unsigned,
                       FilterSize:8/unsigned,
                       _Reserved:24,
                       MaybeFilter/binary>> = HeaderData0} ->
                    <<HeaderData:?HEADER_SIZE_B/binary, _/binary>> = HeaderData0,
                    counters:put(CntRef, ?C_OFFSET, NextChId0 + NumRecords),
                    counters:add(CntRef, ?C_CHUNKS, 1),
                    NextPos = Pos + ?HEADER_SIZE_B + FilterSize + DataSize + TrailerSize,

                    ChunkFilter = case MaybeFilter of
                                      <<F:FilterSize/binary, _/binary>> ->
                                          %% filter is of default size or 0
                                          F;
                                      _  when Filter =/= undefined ->
                                          %% the filter is larger than default
                                          case file:pread(Fd, Pos + ?HEADER_SIZE_B,
                                                          FilterSize) of
                                              {ok, F} ->
                                                  F;
                                              eof ->
                                                  throw({end_of_stream, State})
                                          end;
                                      _ ->
                                          <<>>
                                  end,

                    case osiris_bloom:is_match(ChunkFilter, Filter) of
                        true ->
                            {ok, #{chunk_id => NextChId0,
                                   epoch => Epoch,
                                   type => ChType,
                                   crc => Crc,
                                   num_records => NumRecords,
                                   num_entries => NumEntries,
                                   timestamp => Timestamp,
                                   data_size => DataSize,
                                   trailer_size => TrailerSize,
                                   header_data => HeaderData,
                                   filter_size => FilterSize,
                                   next_position => NextPos,
                                   position => Pos}, State};
                        false ->
                            Read = Read0#read{next_offset = NextChId0 + NumRecords,
                                              position = NextPos},
                            read_header0(State#?MODULE_TMP{mode = Read});
                        {retry_with, NewFilter} ->
                            Read = Read0#read{filter = NewFilter},
                            read_header0(State#?MODULE_TMP{mode = Read})
                    end;
                {ok, Bin} when byte_size(Bin) < ?HEADER_SIZE_B ->
                    %% partial header read
                    %% this can happen when a replica reader reads ahead
                    %% optimistically
                    %% treat as end_of_stream
                    {end_of_stream, State};
                eof ->
                    FirstOffset = osiris_log_shared:first_chunk_id(Shared),
                    %% open next segment file and start there if it exists
                    NextChId = max(FirstOffset, NextChId0),
                    %% TODO: replace this check with a last chunk id counter
                    %% updated by the writer and replicas
                    SegFile = make_file_name(NextChId, "segment"),
                    case SegFile == CurFile of
                        true ->
                            %% the new filename is the same as the old one
                            %% this should only really happen for an empty
                            %% log but would cause an infinite loop if it does
                            {end_of_stream, State};
                        false ->
                            case file:open(filename:join(Dir, SegFile),
                                           [raw, binary, read]) of
                                {ok, Fd2} ->
                                    ok = file:close(Fd),
                                    Read = Read0#read{next_offset = NextChId,
                                                      position = ?LOG_HEADER_SIZE},
                                    read_header0(
                                      State#?MODULE_TMP{current_file = SegFile,
                                                    fd = Fd2,
                                                    mode = Read});
                                {error, enoent} ->
                                    {end_of_stream, State}
                            end
                    end;
                {ok,
                 <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   _ChType:8/unsigned,
                   _NumEntries:16/unsigned,
                   _NumRecords:32/unsigned,
                   _Timestamp:64/signed,
                   _Epoch:64/unsigned,
                   UnexpectedChId:64/unsigned,
                   _Crc:32/integer,
                   _DataSize:32/unsigned,
                   _TrailerSize:32/unsigned,
                   _Reserved:32>>} ->
                    %% TODO: we may need to return the new state here if
                    %% we've crossed segments
                    {error, {unexpected_chunk_id, UnexpectedChId, NextChId0}};
                Invalid ->
                    {error, {invalid_chunk_header, Invalid}}
            end;
        false ->
            {end_of_stream, State}
    end.

make_file_name(N, Suff) ->
    lists:flatten(
        io_lib:format("~20..0B.~s", [N, Suff])).

open_new_segment(#?MODULE_TMP{cfg = #cfg{name = Name,
                                     directory = Dir,
                                     counter = Cnt},
                          fd = OldFd,
                          index_fd = OldIdxFd,
                          mode = #write{type = _WriterType,
                                        tail_info = {NextOffset, _}} = Write} =
                 State0) ->
    _ = close_fd(OldFd),
    _ = close_fd(OldIdxFd),
    Filename = make_file_name(NextOffset, "segment"),
    IdxFilename = make_file_name(NextOffset, "index"),
    ?DEBUG_(Name, "~ts", [Filename]),
    {ok, IdxFd} =
        file:open(
            filename:join(Dir, IdxFilename), ?FILE_OPTS_WRITE),
    ok = file:write(IdxFd, ?IDX_HEADER),
    {ok, Fd} =
        file:open(
            filename:join(Dir, Filename), ?FILE_OPTS_WRITE),
    ok = file:write(Fd, ?LOG_HEADER),
    %% we always move to the end of the file
    {ok, _} = file:position(Fd, eof),
    {ok, _} = file:position(IdxFd, eof),
    counters:add(Cnt, ?C_SEGMENTS, 1),

    State0#?MODULE_TMP{current_file = Filename,
                   fd = Fd,
                   %% reset segment_size counter
                   index_fd = IdxFd,
                   mode = Write#write{segment_size = {?LOG_HEADER_SIZE, 0}}}.

throw_missing({error, enoent}) ->
    throw(missing_file);
throw_missing(Any) ->
    Any.

open(File, Options) ->
    throw_missing(file:open(File, Options)).

segment_from_index_file(IdxFile) when is_list(IdxFile) ->
    unicode:characters_to_list(string:replace(IdxFile, ".index", ".segment", trailing));
segment_from_index_file(IdxFile) when is_binary(IdxFile) ->
    unicode:characters_to_binary(string:replace(IdxFile, ".index", ".segment", trailing)).

truncate_invalid_idx_records(IdxFile, SegSize) ->
    % TODO currently, if we have no valid index records,
    % we truncate the segment, even though it could theoretically
    % contain valid chunks. This should never happen in normal
    % operations, since we write to the index first
    % and fsync it first. However, it feels wrong, since we can
    % reconstruct the index from a segment. We should probably
    % add an option to perform a full segment scan and reconstruct
    % the index for the valid chunks.
    SegFile = segment_from_index_file(IdxFile),
    {ok, IdxFd} = open(IdxFile, [raw, binary, write, read]),
    {ok, Pos} = position_at_idx_record_boundary(IdxFd, eof),
    ok = skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos),
    ok = file:truncate(IdxFd),
    file:close(IdxFd).

skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos) ->
    case Pos >= ?IDX_HEADER_SIZE + ?INDEX_RECORD_SIZE_B of
        true ->
            {ok, _} = file:position(IdxFd, Pos - ?INDEX_RECORD_SIZE_B),
            case file:read(IdxFd, ?INDEX_RECORD_SIZE_B) of
                {ok, ?ZERO_IDX_MATCH(_)} ->
                    % trailing zeros found
                    skip_invalid_idx_records(IdxFd, SegFile, SegSize,
                                             Pos - ?INDEX_RECORD_SIZE_B);
                {ok, ?IDX_MATCH(_, _, ChunkPos)} ->
                    % a non-zero index record
                    case ChunkPos < SegSize andalso
                         is_valid_chunk_on_disk(SegFile, ChunkPos) of
                        true ->
                            ok;
                        false ->
                            % this chunk doesn't exist in the segment or is invalid
                            skip_invalid_idx_records(IdxFd, SegFile, SegSize,
                                                     Pos - ?INDEX_RECORD_SIZE_B)
                    end;
                Err ->
                    Err
            end;
        false ->
            %% TODO should we validate the correctness of index/segment headers?
            {ok, _} = file:position(IdxFd, ?IDX_HEADER_SIZE),
            ok
    end.

file_size(Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        {error, enoent} ->
            throw(missing_file)
    end.

file_size_or_zero(Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        {error, enoent} ->
            0
    end.

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
    end.

close_fd(undefined) ->
    ok;
close_fd(Fd) ->
    _ = file:close(Fd),
    ok.

is_valid_chunk_on_disk(SegFile, Pos) ->
    %% read a chunk from a specified location in the segment
    %% then checks the CRC
    case open(SegFile, [read, raw, binary]) of
        {ok, SegFd} ->
            IsValid = case file:pread(SegFd, Pos, ?HEADER_SIZE_B) of
                          {ok,
                           <<?MAGIC:4/unsigned,
                             ?VERSION:4/unsigned,
                             _ChType:8/unsigned,
                             _NumEntries:16/unsigned,
                             _NumRecords:32/unsigned,
                             _Timestamp:64/signed,
                             _Epoch:64/unsigned,
                             _NextChId0:64/unsigned,
                             Crc:32/integer,
                             DataSize:32/unsigned,
                             _TrailerSize:32/unsigned,
                             FilterSize:8/unsigned,
                             _Reserved:24>>} ->
                              DataPos = Pos + FilterSize + ?HEADER_SIZE_B,
                              case file:pread(SegFd, DataPos, DataSize) of
                                  {ok, Data} ->
                                      case erlang:crc32(Data) of
                                          Crc ->
                                              true;
                                          _ ->
                                              false
                                      end;
                                  eof ->
                                      false
                              end;
                          _ ->
                              false
                      end,
            _ = file:close(SegFd),
            IsValid;
       _Err ->
            false
    end.
