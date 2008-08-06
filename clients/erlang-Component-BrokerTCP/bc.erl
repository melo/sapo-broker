-module(bc).


-import(gen_tcp).
-import(lists).
-import(xmerl_scan).


-export([init/1,subscribe/2,publish/3,receive_msg/1,receive_loop/2,go/0]).


-include("xmerl.hrl").

%-record(item,{topic,type}).


loop() ->
	receive 
		{_From, Data} -> 
			%io:format("go: Result (~p)~n",[Data]),
			{value, {_, Text}} = lists:keysearch('TextPayload',1,Data),
			io:format("go: Result text (~s)~n",[Text]),
			loop()
	end
.


go() ->
	{ok, Desc} = init([{host,"10.135.5.110"}]),
	Ndesc = subscribe(Desc, [{"/sapo/messenger/raw/sessions",'TOPIC_AS_QUEUE'},
		{"/sapo/messenger/raw/presences",'TOPIC_AS_QUEUE'}]),
	%X= receive_msg(Nhandle),
	%io:format("go: Result (~p)~n",[X])

	Pid = spawn(fun loop/0),
	receive_loop(Ndesc,Pid)
.


init(Conf) when is_list(Conf) -> 
	case lists:keysearch(name,1,Conf) of
		{value, {_, Name}} -> Name;
		_ -> {_, Name} = inet:gethostname()
	end,

	case lists:keysearch(host,1,Conf) of
		{value, {_, Host}} -> Host;
		_ -> Host = "localhost"
	end,

	case lists:keysearch(port,1,Conf) of
		{value, {_, Port}} -> Port;
		_ -> Port = 3322
	end,

	case lists:keysearch(timeout,1,Conf) of
		{value, {_, Timeout}} -> Timeout;
		_ -> Timeout = 10000
	end,

	case gen_tcp:connect(
			Host, Port, [binary, {packet, 4}, {active,false}], Timeout) of
		{ok, Socket} -> {ok, {Socket, Name, Timeout, []}};
		{_, X}  -> {error, X}
	end
.		


publish({Socket, Name, _Timeout, _}, {Topic,MT}, Data) -> 
	case MT of 
		'TOPIC_AS_QUEUE' -> Dest = Name ++ "@" ++ Topic;
		_ -> Dest = Topic
	end,

 Msg = 
  "<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'>
	<soapenv:Body><Publish xmlns='http://services.sapo.pt/broker'><BrokerMessage>
  <DestinationName>" ++ escape(Dest) ++ "</DestinationName>
	<TextPayload>" ++ escape(Data) ++ "/TextPayload></BrokerMessage></Publish>
  </soapenv:Body></soapenv:Envelope>",

	gen_tcp:send(Socket,Msg)
.


subscribe({Socket, Name, Timeout, L}, [{Topic,MT}|T]) -> 
	case MT of 
		'TOPIC_AS_QUEUE' -> Dest = Name ++ "@" ++ Topic;
		_ -> Dest = Topic
	end,
	
	Msg = 
		"<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'>
		<soapenv:Body><Notify xmlns='http://services.sapo.pt/broker'>
		<DestinationName>" ++ escape(Dest) ++ "</DestinationName>
		<DestinationType>" ++ atom_to_list(MT) ++ "</DestinationType>
		</Notify></soapenv:Body></soapenv:Envelope>",
  
	L1 = [{Topic, MT} | L],
	 
	case gen_tcp:send(Socket,Msg) of
		ok -> subscribe({Socket, Name, Timeout, L1}, T);
		What -> 
			io:format("subscribe: send failed ~p~n",[What]),
			What
	end;

subscribe(H, []) -> H.


receive_msg({Socket, _, Timeout, L}) -> 
	case gen_tcp:recv(Socket,0,Timeout) of
		{ok, Data} ->
			{XML, _} = xmerl_scan:string(binary_to_list(Data)),
			RV = find_elements(
				['TextPayload', 'MessageId', 'DestinationName'], XML#xmlElement.content),

			acknowledge(Socket, L, RV),
			RV;

		{error, closed} -> []
	end
.


receive_loop(X, Pid) when is_pid(Pid) -> 
	receive_loop(X, fun(Data) -> Pid ! {self(), Data} end)
;

receive_loop({Socket, _N, _Timeout, L}, F) -> 
	%io:format("receive_loop: L ~p~n",[L]),
	case gen_tcp:recv(Socket,0) of
		{ok, Data} -> 
			{XML, _} = xmerl_scan:string(binary_to_list(Data)),
			
			RV = find_elements(
				['TextPayload', 'MessageId', 'DestinationName'], XML#xmlElement.content),

			try F(RV)
			catch
				throw:X -> io:format("receive_loop: function raised exception ~p~n",[X])
			end,

			acknowledge(Socket, L, RV),
			receive_loop({Socket, _N, _Timeout, L}, F);
			
		{error, closed} -> [];
		
		{error, _} ->
			receive_loop({Socket, _N, _Timeout, L}, F)
	end
.


acknowledge(Socket, L, Data) ->
	{value, {_, Dest}} = lists:keysearch('DestinationName',1,Data),	
	[Topic|_] = lists:reverse(string:tokens(Dest,"@")),
	
	%io:format("acknowledge: list ~p~n",[L]),
	
	{value, {_, MT}} = lists:keysearch(Topic,1,L),

	if 
		MT =:= 'TOPIC_AS_QUEUE' ->
			{value, {_, Id}} = lists:keysearch('MessageId',1,Data),

			%io:format("acknowledge: list Dest ~p id ~p~n",[Dest,Id]),

			Msg = 
				"<soapenv:Envelope xmlns:soapenv='http://www.w3.org/2003/05/soap-envelope'>
				<soapenv:Body><Acknowledge xmlns='http://services.sapo.pt/broker'>
				<DestinationName>" ++ escape(Dest) ++ "</DestinationName>
				<MessageId>" ++ Id ++ "</MessageId></Acknowledge>
				</soapenv:Body></soapenv:Envelope>",

			gen_tcp:send(Socket,Msg);

		true -> ok
	end
.

	
find_elements([H|T],L) ->
	%io:format("find_elements: TAG ~p~n",[H]),
	lists:append(find_element(H,L),find_elements(T,L))
;

find_elements([],_) -> []
.


find_element(Tag,[X|T]) ->
	%io:format("find_element: TAG ~p~n",[Tag]),
	case is_record(X,xmlElement) of
		true -> 
			%io:format("find_element: IS ELEMENT name ~p~n",[X#xmlElement.name]),
			case string:str(atom_to_list(X#xmlElement.name),atom_to_list(Tag)) of
				%regexp:first_match(atom_to_list(X#xmlElement.name),atom_to_list(Tag)) of
				0 -> lists:append(find_element(Tag,X#xmlElement.content),find_element(Tag,T));

				_ -> 
					%io:format("find_element: IS ELEMENT found ~p~n",[Tag]),
					[{ Tag, text_content(X#xmlElement.content) } | find_element(Tag,T) ]
					
			end;
		_ -> 	
			%io:format("find_element: case 3~n",[]),
			find_element(Tag,T)
	end
;	

find_element(_,[]) -> []
.


text_content([X|T]) ->
	case is_record(X,xmlText) of
		true -> 
			lists:append(X#xmlText.value,text_content(T));
		false -> 	
			text_content(T)
	end
;	

text_content([]) -> []
.


escape([H|L]) ->
	case H of
		$< -> [$&, $l, $t, $; | escape(L)];
		$& -> [$&, $a, $m, $p, $; | escape(L)];
		_ -> [H | escape(L)]
	end
;

escape([]) -> []
.
