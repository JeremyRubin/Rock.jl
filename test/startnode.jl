using Rock
peers = [(Rock.Hostname("localhost"), 8000), (Rock.Hostname("localhost"), 8001), (Rock.Hostname("localhost"), 8002)]
type C <: Rock.ExternalCriticalCommand
    a::Int
end
function t(c)
@show c
end

i = parse(Int64, ARGS[1])
try
    x = parse(Int, ARGS[2])
    Rock.command(Rock.client("localhost", 8000), C(x))
catch
    Rock.node(Rock.Context(peers[i][2], peers, t, AbstractString("sqlite$i.db")),i)
end
