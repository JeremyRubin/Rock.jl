
Rock.jl
=============

 An implementation of Paxos in Julia with an example application. Work In Progress, not ready for production.

# Usage Examples

Try running the example in src/node.jl by doing the following in 4 different shells:

```export ROCK_PATH=/tmp/rock1 && export ROCK_PORT=8081 && julia node.jl```

```export ROCK_PATH=/tmp/rock2 && export ROCK_PORT=8082 && julia node.jl```

```export ROCK_PATH=/tmp/rock3 && export ROCK_PORT=8083 && julia node.jl```

```export ROCK_PATH=/tmp/rock0 && export ROCK_PORT=8080 && julia node.jl```