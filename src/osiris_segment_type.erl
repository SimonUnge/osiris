-module(osiris_segment_type).

-include("osiris.hrl").
-include_lib("kernel/include/file.hrl").

-export([init/0]).

-type state() :: term().

-define(STATE, ?MODULE).
-record(?STATE, {todo_content}).

%% -type offset() :: osiris:offset().
%% -type epoch() :: osiris:epoch().
%% -type range() :: empty | {From :: offset(), To :: offset()}.
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


%% Is acceptor needed?
%% -callback init(config(), writer | acceptor) ->
%%     {ok, queue_state()} | {error, Reason :: term()}.

-spec init() -> state().
init() ->
    #?STATE{}.
