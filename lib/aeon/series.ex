defmodule Aeon.Series do
  defstruct [:tree, :chunk, :metadata]

  alias Aeon.{Tree, Aggregator, Compressor, Storage}
  alias Aeon.Tree.{Node, Link}

  @type t :: %__MODULE__{tree: Tree.t(), metadata: Aggregator.t(), chunk: Compressor.t()}

  @spec new(pos_integer, Aggregator.t(), Compressor.t(), Storage.Engine.t(), pos_integer) :: t
  def new(id, chunk, metadata, store, fanout \\ 32) do
    %__MODULE__{
      tree: %Tree{id: id, nodes: [], store: store, fanout: fanout},
      chunk: chunk,
      metadata: metadata
    }
  end

  def append(series, item) do
    case Compressor.append(series.chunk, item) do
      {:ok, chunk} ->
        metadata = Aggregator.insert(series.metadata, item)
        {:ok, %{series | metadata: metadata, chunk: chunk}}

      :overflow ->
        with {:ok, tree} <- Tree.append(series.tree, series.chunk, series.metadata) do
          chunk = Compressor.clear(series.chunk)
          metadata = Aggregator.clear(series.metadata)
          series = %{series | tree: tree, metadata: metadata, chunk: chunk}
          append(series, item)
        end
    end
  end

  def points(series) do
    transform(series, fn _ -> :unroll end, &Enum.to_list/1, [], &++/2)
  end

  def metadata(series) do
    transform(series, &{:return, &1}, nil, Aggregator.clear(series.metadata), &Aggregator.merge/2)
  end

  def transform(series = %{tree: tree}, decide, roll, unit, combine) do
    chunk_link = %Link{metadata: series.metadata, offset: 0}

    {nodes, link} =
      Enum.reduce(tree.nodes, {[], chunk_link}, fn node, {nodes, link} ->
        node = Node.append(node, link)
        link = %Link{metadata: Node.metadata(node), offset: -node.level}
        {[node | nodes], link}
      end)

    nodes = Enum.reverse(nodes)
    root = %Node{level: length(nodes) + 1, previous: 0, links: [link]}

    read = fn offset, level ->
      cond do
        offset > 0 ->
          Storage.read(tree.store, offset, tree.id, level)

        offset == 0 ->
          {:ok, series.chunk}

        true ->
          {:ok, Enum.at(nodes, -offset - 1)}
      end
    end

    Node.transform(root, read, decide, roll, unit, combine)
  end
end
