%%%-------------------------------------------------------------------
%% @doc dqliterl top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dqliterl_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

%% API functions

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% supervisor callbacks

init([]) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, child_specs()}}.

%% internal functions

-spec child_specs() -> [supervisor:child_spec()].
child_specs() ->
    [
        #{
            id => dqliterl_node_sup,
            start => {dqliterl_node_sup, start_link, []}
        }
    ].
