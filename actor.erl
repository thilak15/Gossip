-module(actor).
-import(math, []).
-export[start/0, startActors/1, startActorsPushSum/2, startGossip/2, sendGossip/5, sendPushSumMessages/8].

start() ->

    {ok, [Nodes]} = io:fread("\nEnter number of nodes: ", "~d\n"),
    {ok, [Topology]} = io:fread("\nEnter Topology: ", "~s\n"),
    {ok, [Algorithm]} = io:fread("\nEnter the type of Algorithm (Gossip or PushSum): ", "~s\n"),

    if
        Topology == "2D" ->
            No_of_nodes = getNextSquare(Nodes);
        Topology == "3D" ->
            No_of_nodes = getNextSquare(Nodes);
        true ->
            No_of_nodes = Nodes
    end,

    io:format("Number of Nodes: ~p\n", [No_of_nodes]), % Where numNodes is the number of actors involved
    io:format("Topology: ~p\n", [Topology]), % Topology Options: Full Network, 2D Grid, Line, Imperfect 3D Grid
    io:format("Algorithm: ~p\n", [Algorithm]), % Algorithm Options: Gossip, Push-Sum

    case Algorithm of
        "gossip" -> startGossip(No_of_nodes, Topology);
        "pushsum" -> startPushSum(No_of_nodes, Topology)
    end.

startGossip(No_of_nodes, Topology) ->
    io:format('Starting the Gossip Algorithm \n'),
    Actors = createActors(No_of_nodes),
    {ChosenActor, ChosenActor_PID} = lists:nth(rand:uniform(length(Actors)), Actors),
    io:format("\nThe Actor Chosen by the Main Process is : ~p \n\n", [ChosenActor]),

    %start time
    Start_Time = erlang:system_time(millisecond),
    ChosenActor_PID ! {self(), {Topology, Actors, No_of_nodes}},
    checkAliveActors(Actors),
    %End time
    io:format("All Processes received the rumor 10 times.\n"),
    End_Time = erlang:system_time(millisecond),
    io:format("\nTime Taken in milliseconds: ~p\n", [End_Time - Start_Time]).

checkAliveActors(Actors) ->
    Alive_Actors = [{A, A_PID} || {A, A_PID} <- Actors, is_process_alive(A_PID) == true],

    if
        Alive_Actors == [] ->
            io:format("\nCONVERGED: ");
        true ->
            checkAliveActors(Actors)
    end.

getAliveActors(Actors) ->
    Alive_Actors = [{A, A_PID} || {A, A_PID} <- Actors, is_process_alive(A_PID) == true],
    Alive_Actors.

startPushSum(No_of_nodes, Topology) ->
    
    io:format('Starting the Push Sum Algorithm \n'),
    
    W = 1,
    Actors = createActorsPushSum(No_of_nodes, W),
    {ChosenActor, ChosenActor_PID} = lists:nth(rand:uniform(length(Actors)), Actors),
    io:format("\nThe chosen actor is : ~p \n", [ChosenActor]),
    
    %start time
    Start_Time = erlang:system_time(millisecond),

    ChosenActor_PID ! {self(), {0, 0, Topology, Actors, No_of_nodes, self()}},
    checkAliveActors(Actors),
    io:format("All Processes converged with the same s/w ratio.\n"),
    End_Time = erlang:system_time(millisecond),
    io:format("\nTime Taken in milliseconds: ~p\n", [End_Time - Start_Time]).

buildTopology(Topology, Actors, No_of_nodes, Id) ->
    Actors_Map = maps:from_list(Actors),
    case Topology of
        "full" -> findFullNetworkNeighbors(Id, No_of_nodes, Actors_Map);
        "2D" -> find2DGridNeighbors(Id, No_of_nodes, Actors_Map);
        "line" -> findLineGridNeighbors(Id, No_of_nodes, Actors_Map);
        "3D" -> findImperfect3DGridNeighbors(Id, No_of_nodes, Actors_Map)
    end.

findFullNetworkNeighbors(Id, N, Actors_Map) ->
    % considers everyone except itself for neighbors
    Neighbors = lists:subtract(lists:seq(1, N), [Id]),
    Detailed_Neighbors = [
        {N, maps:get(N, Actors_Map)}
        || N <- Neighbors, maps:is_key(N, Actors_Map)
    ],
    Detailed_Neighbors.

find2DGridNeighbors(Id, N, Actors_Map) ->

    % assumption provided in the question is that the 2D grid is always a perfect square.
    Rows = erlang:trunc(math:sqrt(N)),
    ModVal = Id rem Rows,

    if
        ModVal == 1 ->
            Neighbors = [Id+1];
        ModVal == 0 ->
            Neighbors = [Id-1];
        true ->
            Neighbors = lists:append([[Id-1], [Id+1]])
    end,

    if
        Id+Rows > N ->
            Neighbors2 = Neighbors;
        true ->
            Neighbors2 = lists:append([Neighbors, [Id+Rows]])
    end,
    if
        Id-Rows < 1 ->
            Neighbors3 = Neighbors2;
        true ->
            Neighbors3 = lists:append([Neighbors2, [Id-Rows]])
    end,
    
    Detailed_Neighbors = [
        {N, maps:get(N, Actors_Map)}
        || N <- Neighbors3, maps:is_key(N, Actors_Map)
    ],
    Detailed_Neighbors.

findLineGridNeighbors(Id, N, Actors_Map) ->

    if
        Id > N -> 
            Neighbors = [];
        Id < 1 ->
            Neighbors = [];
        Id + 1 > N ->
            if
                Id - 1 < 1 ->
                    Neighbors = [];
                true ->
                    Neighbors = [Id-1]
            end;           
        true ->
            if
                Id - 1 < 1 ->
                    Neighbors = [Id+1];
                true ->
                    Neighbors = [Id-1, Id+1]
            end
    end,
    Detailed_Neighbors = [
        {N, maps:get(N, Actors_Map)}
        || N <- Neighbors, maps:is_key(N, Actors_Map)
    ],
    Detailed_Neighbors.

findImperfect3DGridNeighbors(Id, N, Actors_Map) ->
    
    Rows = erlang:trunc(math:sqrt(N)),
    ModVal = Id rem Rows,

    if
        ModVal == 1 ->
            Neighbors = [Id+1];
        ModVal == 0 ->
            Neighbors = [Id-1];
        true ->
            Neighbors = lists:append([[Id-1], [Id+1]])
    end,

    if
        Id+Rows > N ->
            Neighbors2 = Neighbors;
        true ->
            if 
                ModVal == 1 ->
                    Neighbors2 = lists:append([Neighbors, [Id+Rows], [Id+Rows+1]]);
                ModVal == 0 ->
                    Neighbors2 = lists:append([Neighbors, [Id+Rows], [Id+Rows-1]]);
                true ->
                    Neighbors2 = lists:append([Neighbors, [Id+Rows], [Id+Rows-1], [Id+Rows+1]])
            end
    end,
    if
        Id-Rows < 1 ->
            ImmediateNeighbors = Neighbors2;
        true ->
            if 
                ModVal == 1 ->
                    ImmediateNeighbors = lists:append([Neighbors2, [Id-Rows], [Id-Rows+1]]);
                ModVal == 0 ->
                    ImmediateNeighbors = lists:append([Neighbors2, [Id-Rows], [Id-Rows-1]]);
                true ->
                    ImmediateNeighbors = lists:append([Neighbors2, [Id-Rows], [Id-Rows-1], [Id-Rows+1]])
            end
    end,

    NeighborsToBeIgnored = lists:append([ImmediateNeighbors, [Id]]),
    RemainingNeighbors = lists:subtract(lists:seq(1, N), NeighborsToBeIgnored),

    RandomRemaningNeighbor = lists:nth(rand:uniform(length(RemainingNeighbors)), RemainingNeighbors),
    %RandomImmediateNeighbor = lists:nth(rand:uniform(length(ImmediateNeighbors)), ImmediateNeighbors),

    FinalNeighbors = lists:append([[RandomRemaningNeighbor], ImmediateNeighbors]),

    Detailed_Neighbors = [
        {N, maps:get(N, Actors_Map)}
        || N <- FinalNeighbors, maps:is_key(N, Actors_Map)
    ],
    Detailed_Neighbors.

startActors(Id) ->
    awaitResponseGossip(Id, 0).

awaitResponseGossip(Id, Count) ->
    receive
        {From, {Topology, Actors, No_of_nodes}} ->
            if
                Count == 10 ->
                    %io:format("CONVERGED: in Process: ~p || Count: ~p\n", [Id, Count]);
                    exit(0);
                true ->
                    spawn(actor, sendGossip, [self(), Topology, Actors, No_of_nodes, Id]),
                    awaitResponseGossip(Id, Count+1)
            end
    end.

sendGossip(Current, Topology, Actors, No_of_nodes, Id) ->
    Status = is_process_alive(Current),
    if
        Status == true ->
            Alive_Actors = getAliveActors(Actors),

            Neighbors = buildTopology(Topology, Alive_Actors, No_of_nodes, Id),
            if
                Neighbors == [] ->
                    exit(0);
                true ->
                    {_, ChosenNeighbor_PID} = lists:nth(rand:uniform(length(Neighbors)), Neighbors),
                    ChosenNeighbor_PID ! {Current, {Topology, Actors, No_of_nodes}},
                    sendGossip(Current, Topology, Actors, No_of_nodes, Id)
            end;
        true ->
            exit(0)
    end.  
    

createActors(N) ->

    Actors = [  % { {Pid, Ref}, Id }
        {Id, spawn(actor, startActors, [Id])}
        || Id <- lists:seq(1, N)
    ],

    Actors.

createActorsPushSum(N, W) ->
    Actors = [  % { {Pid, Ref}, Id }
        {Id, spawn(actor, startActorsPushSum, [Id, W])}
        || Id <- lists:seq(1, N)
    ],
    Actors.

startActorsPushSum(Id, W) ->
    %io:fwrite("I am an actor with Id : ~w\n", [Id]),
    awaitResponsePushSum(Id, Id, W, 0, 0, self()).


awaitResponsePushSum(Id, S, W, Prev_ratio, Count, Last_Spawned_Process_Id) ->
    receive
        {From, {S1, W1, Topology, Actors, No_of_nodes, Main}} ->

            if
                Count > 1 ->
                    exit(0);
                true ->
                    % Upon receiving this the actor should add the received pair to its own corresponding values
                    S2 = S + S1,
                    W2 = W + W1,
                    
                    %Upon receiving, each actor selects a random neighbor and sends it a message.
                    Alive_Actors = getAliveActors(Actors),
                    Neighbors = buildTopology(Topology, Alive_Actors, No_of_nodes, Id),

                    if
                        Neighbors == [] ->
                            exit(0);
                        true ->

                            % SEND: When sending a message to another actor, half of s and w is kept by the sending actor and half is placed in the message
                            S3 = S2 / 2,
                            W3 = W2 / 2,

                            if
                                From == Main ->
                                    Spawned_Process_Id = spawn(actor, sendPushSumMessages, [self(), Actors, Topology, No_of_nodes, Id, S3, W3, Main]);
                                true ->
                                    Last_Spawned_Process_Id ! {exit},
                                    Spawned_Process_Id = spawn(actor, sendPushSumMessages, [self(), Actors, Topology, No_of_nodes, Id, S3, W3, Main])
                            end,
                            
                            Curr_ratio = S / W,
                            Difference = math:pow(10, -10),
                            if
                                abs(Curr_ratio - Prev_ratio) < Difference ->
                                    %io:format("\nPrevious Ratio: ~p & Current Ratio ~p & Difference is ~p\n",[Prev_ratio, Curr_ratio, abs(Curr_ratio - Prev_ratio)]),
                                    awaitResponsePushSum(Id, S3, W3, Curr_ratio, Count + 1, Spawned_Process_Id);
                                true ->
                                    awaitResponsePushSum(Id, S3, W3, Curr_ratio, 0, Spawned_Process_Id)
                            end
                    end                    
            end
    end.

sendPushSumMessages(Current, Actors, Topology, No_of_nodes, Id, S3, W3, Main) ->

    receive
        _->
            exit(0)
    after 0 ->
        Status = is_process_alive(Current),
        if
            Status == true ->
                Alive_Actors = getAliveActors(Actors),
                Neighbors = buildTopology(Topology, Alive_Actors, No_of_nodes, Id),
                if
                    Neighbors == [] ->
                        exit(0);
                    true ->
                        {_, ChosenNeighbor_PID} = lists:nth(rand:uniform(length(Neighbors)), Neighbors),
                        % SEND: When sending a message to another actor, half of s and w is kept by the sending actor and half is placed in the message                     
                        ChosenNeighbor_PID ! {self(), {S3, W3, Topology, Actors, No_of_nodes, Main}},
                        sendPushSumMessages(Current, Actors, Topology, No_of_nodes, Id, S3, W3, Main)
                end;
            true ->
                exit(0)
        end
    end.

getNextSquare(No_of_nodes) ->
    SquaredNumber =  round(math:pow(math:ceil(math:sqrt(No_of_nodes)),2)),
    SquaredNumber.


