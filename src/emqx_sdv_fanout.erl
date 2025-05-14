-module(emqx_sdv_fanout).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx_plugin_helper/include/emqx.hrl").

%% for hook priority constants
-include_lib("emqx_plugin_helper/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx_plugin_helper/include/logger.hrl").

-include("emqx_sdv_fanout.hrl").

-export([
    hook/0,
    unhook/0,
    start_link/0
]).

-export([
    on_config_changed/2,
    on_health_check/1,
    get_config/0
]).

%% Hook callbacks
-export([
    on_message_publish/1,
    on_delivery_completed/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SDV_FANOUT_TOPIC, <<"$SDV-FANOUT">>).

%% @doc
%% Called when the plugin application start
hook() ->
    %% Handle batch for fanout from SDV platform
    %% Hook it with higher priority than retainer
    %% which is also higher than rule engine
    %% so the message can be terminated here at this plugin,
    %% but not to leak to retainer or rule engine
    emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    %% Handle PUBACK from subscribers (vehicles)
    emqx_hooks:add('message.puback', {?MODULE, on_message_puback, []}, ?HP_HIGHEST),
    ok.

%% @doc
%% Called when the plugin stops
unhook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    ok.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

%% @doc
%% Called when a message is published by the SDV platform.
on_message_publish(#message{topic = ?SDV_FANOUT_TOPIC, payload = Payload} = Message) ->
    Headers = Message#message.headers,
    case emqx_sdv_fanout_dispatcher:batch(Payload) of
        ok ->
            {stop, Message#message{headers = Headers#{allow_publish => false}}};
        {error, Reason} ->
            %% Disconnect the client, this seems to be the only way to notify the client
            %% about the error, as EMQX message.publish hook does not support returning
            %% PUBACK reason codes.
            ?LOG(error, "failed_to_handle_batch_will_disconnect", Reason),
            {stop, Message#message{headers = Headers#{should_disconnect => true}}}
    end;
on_message_publish(#message{topic = <<"ecp/", Heartbeat/binary>>} = Message) ->
    case emqx_topic:words(Heartbeat) of
        [VIN, <<"online">>] ->
            ok = emqx_sdv_fanout_dispatcher:heartbeat(VIN);
        _ ->
            ok
    end,
    {ok, Message};
on_message_publish(Message) ->
    %% Other topics, non of our business, just pass it on.
    {ok, Message}.

%% @doc
%% Called when PUBACK is received from the subscriber (vehicle).
on_delivery_completed(#message{topic = Topic}, _) ->
    case emqx_topic:words(Topic) of
        [<<"agent">>, VIN, <<"proxy">>, <<"request">>, RequestId] ->
            ok = emqx_sdv_fanout_dispatcher:ack(VIN, RequestId);
        _ ->
            %% Other topics, non of our business, just pass it on.
            ok
    end.

%%--------------------------------------------------------------------
%% Plugin callbacks
%%--------------------------------------------------------------------

%% @doc
%% - Return `{error, Error}' if the health check fails.
%% - Return `ok' if the health check passes.
%%
%% NOTE
%% For demonstration, we consider any port number other than 3306 unavailable.
on_health_check(_Options) ->
    ok.

%% @doc
%% - Return `{error, Error}' if the new config is invalid.
%% - Return `ok' if the config is valid and can be accepted.
%%
%% NOTE
%% We validate only the client_regex field here. Other config fields are present
%% only for the demonstration purposes.
%%
%% NOTE
%% Take the following considerations into account when writing the validating callback:
%% * You should not make validations depending on the environment, e.g.
%%   check network connection, file system, etc.
%% * The callback may be called even if the application is not running.
%%   Here we use `gen_server:cast/2` to react on changes. The cast will be silently
%%   ignored if the plugin is not running.
on_config_changed(_OldConfig, NewConfig) ->
    %% TODO: Validate NewConfig
    ok = gen_server:cast(?MODULE, {on_changed, NewConfig}).

%%--------------------------------------------------------------------
%% Working with config
%%--------------------------------------------------------------------

%% @doc
%% Efficiently get the current config.
get_config() ->
    persistent_term:get(?MODULE, #{}).

%% gen_server callbacks

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    persistent_term:put(?MODULE, Config),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({on_changed, Config}, State) ->
    persistent_term:put(?MODULE, Config),
    %% NOTE
    %% additionally handling of the config change here, i.e
    %% reestablish the connection to the database in case of host change, etc.
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    persistent_term:erase(?MODULE),
    ok.
