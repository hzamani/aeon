defmodule Aeon.Tree.Node do
  defstruct [:level, :previous, :links]

  alias Aeon.{Aggregator, Compressor}

  def append(node, nil), do: node

  def append(node, link) do
    %{node | links: [link | node.links]}
  end

  def metadata(%{links: links}) do
    links
    |> Enum.map(fn link -> link.metadata end)
    |> Enum.reduce(&Aggregator.merge/2)
  end

  def transform(node, read, decide, roll, unit, combine) do
    node.links
    |> Enum.reverse()
    |> Stream.flat_map(fn link ->
      case decide.(link.metadata) do
        {:return, item} ->
          [{:ok, item}]

        :skip ->
          []

        :unroll ->
          case read.(link.offset, node.level - 1) do
            {:ok, node = %__MODULE__{}} ->
              [transform(node, read, decide, roll, unit, combine)]

            {:ok, chunk} ->
              [{:ok, roll.(Compressor.stream(chunk))}]

            {:error, :skip} ->
              []

            {:error, reason} ->
              [{:error, reason}]
          end
      end
    end)
    |> Enum.reduce_while(unit, fn
      {:error, reason}, _acc -> {:halt, {:error, reason}}
      {:ok, item}, acc -> {:cont, combine.(acc, item)}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      result -> {:ok, result}
    end
  end
end
