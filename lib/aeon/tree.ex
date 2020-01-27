defmodule Aeon.Tree do
  defstruct [:id, :nodes, :store, :fanout]

  alias Aeon.Storage
  alias Aeon.Tree.{Node, Link}

  @type t :: %__MODULE__{
          id: pos_integer,
          nodes: [Node.t()],
          store: Storage.Engine.t(),
          fanout: pos_integer
        }

  @spec append(t, Storable.t(), Storable.t()) :: {:ok, t} | {:error, Storage.reason()}
  def append(tree, chunk, metadata) do
    with {:ok, offset} <- Storage.write(tree.store, tree.id, chunk) do
      link = %Link{metadata: metadata, offset: offset}
      push(tree, link, tree.nodes, [])
    end
  end

  @spec root(t) :: {:ok, Node.t()} | :error
  def root(tree) do
    case List.last(tree.nodes) do
      nil -> :error
      node -> {:ok, node}
    end
  end

  @spec push(t, Link.t(), [Node.t()], [Node.t()]) :: {:ok, t} | {:error, Storage.reason()}
  defp push(tree, link, [], in_memory) do
    root = %Node{level: length(in_memory) + 1, previous: 0, links: [link]}
    nodes = Enum.reverse(in_memory, [root])
    {:ok, %{tree | nodes: nodes}}
  end

  defp push(tree, link, [node | tail], in_memory) do
    node = Node.append(node, link)

    if length(node.links) == tree.fanout do
      with {:ok, offset} <- Storage.write(tree.store, tree.id, node) do
        link = %Link{offset: offset, metadata: Node.metadata(node)}
        empty = %Node{level: node.level, previous: offset, links: []}
        in_memory = Enum.map(in_memory, fn node -> %{node | previous: 0} end)
        push(tree, link, tail, [empty | in_memory])
      end
    else
      {:ok, %{tree | nodes: Enum.reverse(in_memory, [node | tail])}}
    end
  end
end
