-module(emqx_sdv_fanout_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ConfigChildSpec = #{
        id => emqx_sdv_fanout,
        start => {emqx_sdv_fanout, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_sdv_fanout]
    },
    SupFlags = #{
        strategy => one_for_all,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlags, [ConfigChildSpec]}}.
