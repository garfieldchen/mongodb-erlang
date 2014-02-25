-module(mongo_pool).
-export([
	start/4,
	start_link/4,
	get/1
]).

-behaviour(gen_server).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-record(state, {
	supervisor  :: pid(),
	connections :: array(),
	monitors    :: orddict:orddict(),
	options		:: list()
}).


-spec start(atom(), pos_integer(), pid(), list()) -> {ok, pid()}.
start(Name, Size, Sup, Options) ->
	gen_server:start({local, Name}, ?MODULE, [Size, Sup, Options], []).

-spec start_link(atom(), pos_integer(), pid(), list()) -> {ok, pid()}.
start_link(Name, Size, Sup, Options) ->
	gen_server:start_link({local, Name}, ?MODULE, [Size, Sup, Options], []).

-spec get(atom() | pid()) -> mongo_connection:connection().
get(Pool) ->
	case gen_server:call(Pool, get, infinity) of
		{ok, Connection} -> Connection;
		{error, Reason} -> erlang:exit(Reason)
	end.

%% @hidden
init([Size, Sup, Options]) ->
	random:seed(erlang:now()),
	{ok, #state{
		supervisor = Sup,
		connections = array:new(Size, [{fixed, false}, {default, undefined}]),
		options = Options,
		monitors = orddict:new()
	}}.

%% @hidden
handle_call(get, _From, #state{connections = Connections, options = Options, supervisor = Sup} = State) ->
	Index = random:uniform(array:size(Connections)) - 1,
	case array:get(Index, Connections) of
		undefined ->
			case supervisor:start_child(Sup, []) of
				{ok, Connection} ->
					case auth(Connection, Options) of
						true ->
							Monitor = erlang:monitor(process, Connection),
							{reply, {ok, Connection}, State#state{
								connections = array:set(Index, Connection, Connections),
								monitors = orddict:store(Monitor, Index, State#state.monitors)
							}};
						E ->
							supervisor:terminate_child(Sup, Connection),
							{reply, E, State}
					end;
				{error, Error} ->
					{reply, {error, Error}, State}
			end;
		Connection ->
			{reply, {ok, Connection}, State}
	end.

%% @hidden
handle_cast(_, State) ->
	{noreply, State}.

%% @hidden
handle_info({'DOWN', Monitor, process, _Pid, _}, #state{monitors = Monitors} = State) ->
	{ok, Index} = orddict:find(Monitor, Monitors),
	{noreply, State#state{
		connections = array:set(Index, undefined, State#state.connections),
		monitors = orddict:erase(Index, Monitors)
	}};

handle_info(_, State) ->
	{noreply, State}.

%% @hidden
terminate(_, _State) ->
	ok.

%% @hidden
code_change(_Old, State, _Extra) ->
	{ok, State}.


auth(Connection, Options) ->
	Auth = fun({DB, U, P}) ->
				mongo:do(unsafe, master, Connection, DB, fun() -> mongo:auth(U, P) end)
			end,

	CheckOk = fun(Doc) ->
				case bson:at(ok, Doc) of
					N when N == 1 ->
						true;
					true ->
						true;
					_ ->
						false
				end
			end,

	case proplists:get_value(auth, Options) of
		undefined ->
			true;
		{_, _, _} = D ->
			CheckOk(Auth(D));
		L ->
			lists:foldl(fun (D, true) -> Auth(D);
							(_, false) -> false
						end, true, L)
	end.
