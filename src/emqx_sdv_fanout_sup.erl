-module(emqx_sdv_fanout_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include("emqx_sdv_fanout.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 100,
        period => 10
    },
    ConfigChildSpec = #{
        id => emqx_sdv_fanout,
        start => {emqx_sdv_fanout, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_sdv_fanout]
    },
    PoolModule = emqx_sdv_fanout_dispatcher,
    PoolType = hash,
    PoolSize = resolve_pool_size(),
    MFA = {PoolModule, start_link, []},
    Pool = PoolModule,
    SupArgs = [Pool, PoolType, PoolSize, MFA],
    PoolSupSpec = emqx_pool_sup:spec(emqx_sdv_fanout_dispatcher_sup, SupArgs),
    {ok, {SupFlags, [ConfigChildSpec, PoolSupSpec]}}.

resolve_pool_size() ->
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    ConfigedSize = maps:get(<<"dispatcher_pool_size">>, Config),
    resolve_pool_size(ConfigedSize).

resolve_pool_size(0) ->
    erlang:system_info(schedulers);
resolve_pool_size(N) when is_integer(N) ->
    N.
