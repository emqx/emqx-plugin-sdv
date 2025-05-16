%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_config).

-compile({no_auto_import, [get/0, put/1]}).
-export([get/0, put/1, parse/1]).
-export([get_data_retention/0]).

-include("emqx_sdv.hrl").

-spec get() -> map().
get() ->
    persistent_term:get(?MODULE).

-spec put(map()) -> ok.
put(Parsed) ->
    persistent_term:put(?MODULE, Parsed).

%% @doc Parse the config.
%% Avro schema does certain validation for the config,
%% but we still need to convert the config to the internal format
%% and also some extra validation like value range check.
-spec parse(map()) -> map().
parse(Config) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            {Key1, Parsed} = parse(Key, Value),
            maps:put(Key1, Parsed, Acc)
        end,
        #{},
        Config
    ).

%% @doc Get the data retention period.
%% Duration in milliseconds.
-spec get_data_retention() -> integer().
get_data_retention() ->
    maps:get(data_retention, get()).

%% @doc Get the garbage collection interval.
%% Duration in milliseconds.
-spec get_gc_interval() -> integer().
get_gc_interval() ->
    maps:get(gc_interval, get()).

parse(<<"data_retention">>, Str) ->
    {data_retention, to_duration_ms(Str)};
parse(<<"gc_interval">>, Str) ->
    {gc_interval, to_duration_ms(Str)};
parse(<<"dispatcher_pool_size">>, Size) ->
    (Size < 0 orelse Size > 10240) andalso throw("invalid_dispatcher_pool_size"),
    {dispatcher_pool_size, Size}.

to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        D when is_number(D) ->
            erlang:ceil(D);
        _ ->
            case to_integer(Str) of
                I when is_integer(I) -> I;
                _ -> throw("invalid_duration_for_data_retention")
            end
    end.

to_integer(Str) when is_binary(Str) ->
    case string:to_integer(Str) of
        {Int, <<>>} -> Int;
        _ -> error
    end.
