module RPC
typealias SlotNum Int
typealias Hostname AbstractString
typealias Port Int64
# Base Message Type, not too useful
abstract BaseMessage
# Command type; cause some action to happen on a server
abstract Command <: BaseMessage
# Resonse as a result of causing a command to happen
abstract Response <: BaseMessage
# Commands that do not need replication
abstract NonCriticalCommand <: Command
# Commands that take a user defined function and run them, without consensus
abstract ExternalNonCriticalCommand <: NonCriticalCommand
abstract InternalNonCriticalCommand <: NonCriticalCommand
# Critical commands need fault tolerance, not instance specific
abstract CriticalCommand <: Command
# Internal critical commands for actions such as membership changes
abstract InternalCriticalCommand <: CriticalCommand
# External critical commands for the user subclass
abstract ExternalCriticalCommand <: CriticalCommand
# Non Critical for consistency, but critical for progress/consensus forming messages.
abstract ConsensusCommand <: Command
abstract ConsensusResponse <: Response
#####################################
##    Components of the log        ##
#####################################
@enum Fate Decided Pending Forgotten
type Slot
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{CriticalCommand}
    fate::Fate
end


#####################################
#   Core Command Section            #
#####################################




type PrepareArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
end

type PrepareReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    n_a::Int64
    v_a::Nullable{CriticalCommand}
end
type AcceptArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
    v::CriticalCommand
end
type AcceptReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    v::CriticalCommand
end
type DecideArgs <: ConsensusCommand
    slot_num::SlotNum
    v::CriticalCommand
end
type DecideReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
end

#########################
## Other msg Types     ##
#########################
type Ping <: InternalNonCriticalCommand
    msg::ASCIIString
end
type PingReply <: Response
end

# TODO
type DeadLeader <: InternalCriticalCommand
    id::Int64
end

type AddPeer <: InternalCriticalCommand
    new::Tuple{Hostname, Port}
end
type DeletePeer <: InternalCriticalCommand
    dead_peer::Tuple{Hostname, Port}
end
type SwapPeer <: InternalCriticalCommand
    old::DeletePeer
    new::AddPeer
end
type NoOp <: InternalCriticalCommand
end
type ExtraInfo
    completed::Array{Int64, 1}
end

immutable WithExtraInfo <: BaseMessage
    command::BaseMessage
    info::ExtraInfo
end
end
