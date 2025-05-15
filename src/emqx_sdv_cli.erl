%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_cli).

-export([cmd/1]).

-include("emqx_sdv.hrl").

cmd(["show-config" | More]) ->
    emqx_ctl:print("~ts", [format_config(More)]);
cmd(_) ->
    emqx_ctl:usage([usage("show-config")]).

format_config([]) ->
    format_config(["origin"]);
format_config(["origin" | MaybeJSON]) ->
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    case MaybeJSON of
        ["--json" | _] ->
            [emqx_utils_json:encode(Config), "\n"];
        _ ->
            hocon_pp:do(Config, #{})
    end;
format_config(["inuse" | MaybeJSON]) ->
    Config = emqx_sdv_config:get(),
    case MaybeJSON of
        ["--json" | _] ->
            [emqx_utils_json:encode(Config), "\n"];
        _ ->
            io_lib:format("~p~n", [Config])
    end;
format_config(Args) ->
    emqx_ctl:print("bad args for show-config: ~p~n", [Args]),
    emqx_ctl:usage([usage("show-config")]).

usage("show-config") ->
    {"show-config [origin|inuse] [--json]",
        "Show current config, 'origin' for original config, "
        "'inuse' for in-use (parsed) config, add '--json' for JSON format."}.
