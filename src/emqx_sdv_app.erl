%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1,
    on_handle_api_call/4
]).

-include("emqx_sdv.hrl").

start(_StartType, _StartArgs) ->
    create_tables(),
    {ok, Sup} = emqx_sdv_sup:start_link(),
    emqx_sdv:hook(),
    emqx_ctl:register_command(sdv, {emqx_sdv_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(sdv),
    emqx_sdv:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_sdv:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_sdv:on_health_check(Options).

on_handle_api_call(Method, PathRemainder, Request, Context) ->
    log_api_callback(Method, PathRemainder, Request, Context),
    {ok, 200, #{}, #{code => <<"NOT_IMPLEMENTED">>, message => <<"Demo API callback">>}}.

create_tables() ->
    ok = emqx_sdv_fanout_data:create_tables(),
    ok = emqx_sdv_fanout_ids:create_tables(),
    ok.

log_api_callback(Method, PathRemainder, Request, Context) when
    Method =:= get;
    Method =:= post;
    Method =:= put;
    Method =:= patch;
    Method =:= delete;
    Method =:= head;
    Method =:= options
->
    ?LOG(warning, "demo_api_callback_invoked", #{
        method => Method,
        path => PathRemainder,
        request => Request,
        context => Context
    });
log_api_callback(Method, PathRemainder, Request, Context) ->
    ?LOG(warning, "demo_api_callback_invoked_unknown_method", #{
        method => Method,
        path => PathRemainder,
        request => Request,
        context => Context
    }).
