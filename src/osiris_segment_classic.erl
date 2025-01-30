-module(osiris_segment_classic).

-include("osiris.hrl").
-include("osiris_log.hrl").
-include_lib("kernel/include/file.hrl").

-export([chunk_iterator/1,
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

%% TODO fix later, just to make tests pass. Need to up date the init method
-record(osiris_log,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename_all(),
         index_fd :: undefined | file:io_device(),
         fd :: undefined | file:io_device()
        }).

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
