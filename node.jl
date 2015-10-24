module Rock
export StartCluster
export StartNode
export ServerId
typealias ServerId Int64
typealias  LogIndex Int64
typealias Term Int64

type LogEntry{Command}
    term::Term
    command::Command
end
type PersistentState{Command}
    currentTerm::Term
    votedFor::Nullable{ServerId}
    log::Array{Nullable{LogEntry{Command}},1}
end

type VolatileState
    commitIndex::LogIndex
    lastApplied::LogIndex
end

type VolatileLeaderState
    nextIndex::Dict{ServerId, LogIndex}
    matchIndex::Dict{ServerId, LogIndex}
end
type State{Command}
    persistent::PersistentState{Command}
    volatile::VolatileState
    leader::Nullable{VolatileLeaderState}
end

abstract RPCArgs{Command}




@enum Role Follower Candidate Leader
type Node{Command}
    role::Role
    state::State
    id::ServerId
    messages::Channel{RPCArgs}
end


function StartCluster(servers::Array{ServerId, 1})
end

function StartNode(id::ServerId)


    p = PersistentState{Int64}(0,Nullable(0),[])
    vs = VolatileState(0,0)
    vls = Nullable(VolatileLeaderState(Dict(), Dict()))
    Node{Int64}(State(p, vs, vls), id)
end

abstract RPCReply

type AppendArgs{Command} <: RPCArgs{Command}
    term::Term
    leaderId::ServerId
    prevLogIndex::LogIndex
    prevLogTerm::Term
    entries::Array{Tuple{LogIndex, Command},1}
    leaderCommit::LogIndex
end
type AppendReply <: RPCReply

    term::Term
    success::Bool
    end
type VoteArgs <: RPCArgs
    term::Term
    candidateId::ServerId
    lastLogIndex::LogIndex
    lastLogTerm::Term
end
type VoteReply <: RPCReply
    term::Term
    voteGranted::Bool
end

function RPC{Command}(node::Node{Command}, args::AppendArgs{Command})
    r = AppendReply(node.state.persistent.currentTerm, false)
    if args.term < node.state.persistent.currentTerm
        return r
    elseif args.prevLogTerm != node.state.log[args.prevLogIndex].term
        return r
    else
        sort!(args.entries)
        for (i, c) in args.entries
            d = node.state.persistent.log[i]
            if  !d.isnull  && d != c
                nullLogAfter!(node.state.persistent.log, i)
                break
            end
        end

        for (i,c) = args.entries
            addLog!(node.state.persistent.log, i, c)
        end

        if args.leaderCommit > node.state.volatile.commitIndex
            commitIndex = min(args.leaderCommit, e[end][1])
        end
        r.success = true
        return r
    end
end
function RPC{Command}(node::Node{Command},args::VoteArgs)
    r = VoteReply(node.state.persistent.currentTerm, false)
    vf = node.state.persistent.votedFor.isnull
    lastLog = node.state.persistent.log[end]
    if (  !(args.term < node.state.persistent.currentTerm)
        && (vf.isnull || isequal(vf, Nullable(args.candidateId)))
        && lastLog.term <= args.lastLogTerm
        && length(node.state.persistent.log) <= args.lastLogIndex)
        r.voteGranted = true
    end
    return r
end

function statemachine{Command}(node::Node{Command}, command::Command)

end

function reply!(node::Node{Command}, reply::RPCReply)

end
function AllSevers{Command}(node::Node{Command})
    while true
        if node.state.volatile.commitIndex < node.state.volatile.lastApplied
            stateMachine(node, node.state.persistent.log[node.state.volatile.lastApplied].command)
            node.state.volatile.lastApplied +=1
            continue
        end
        if isready(node.messages)
            m = take!(node.messages)
            if m.term> node.state.persistent.currentTerm
                node.role = Follower
                updateTerm!(node, m.term)
            end
            reply!(node, RPC(node,m))
        end
        if node.role == Follower
        elseif node.role == Candidate
            updateTerm!(node, node.state.persistent.currentTerm +1)
        else # node.role == Leader

            
        end
    end

end
end

end


    






import Rock
Rock.StartNode(1)

