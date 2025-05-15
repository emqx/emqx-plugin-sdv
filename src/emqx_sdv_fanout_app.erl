%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

-include("emqx_sdv_fanout.hrl").

start(_StartType, _StartArgs) ->
    create_tables(),
    ok = mria_rlog:wait_for_shards([?DB_SHARD], infinity),
    ok = mria:wait_for_tables([?ID_TAB, ?DATA_TAB]),
    {ok, Sup} = emqx_sdv_fanout_sup:start_link(),
    emqx_sdv_fanout:hook(),
    emqx_ctl:register_command(emqx_sdv_fanout, {emqx_sdv_fanout_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(emqx_sdv_fanout),
    emqx_sdv_fanout:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_sdv_fanout:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_sdv_fanout:on_health_check(Options).

create_tables() ->
    ok = emqx_sdv_fanout_data:create_tables(),
    ok = emqx_sdv_fanout_ids:create_tables(),
    ok.
