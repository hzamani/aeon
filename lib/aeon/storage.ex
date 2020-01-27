defmodule Aeon.Storage do
  alias Aeon.Tree.{Node, Link}
  alias Aeon.Storage.{Engine, Storable}

  @type reason :: File.posix() | :unkown_tag | :retired | :corrupted

  @modules %{
    0x00 => Aeon.Aggregation.Float,
    0x80 => Aeon.Compression.None
  }

  @tags Enum.into(@modules, %{}, fn {tag, module} -> {module, tag} end)

  @spec write(Engine.t(), integer, Storable.t() | Node.t()) ::
          {:ok, Engine.offset()} | {:error, reason}
  def write(store, uid, node = %Node{}) do
    with {:ok, tag, iodata} <- dump_node(node) do
      write_iodata(store, 1, node.level, uid, tag, iodata)
    end
  end

  def write(store, uid, struct = %kind{}) do
    with {:ok, tag} <- tag(kind) do
      write_iodata(store, 1, 0, uid, tag, kind.dump(struct))
    end
  end

  defp write_iodata(store, version, level, uid, tag, iodata) do
    size = IO.iodata_length(iodata)
    checksum = :erlang.crc32(iodata)
    header = <<version::10, level::6, uid::64, tag::8, size::16, checksum::32>>
    Engine.write(store, [header | List.wrap(iodata)])
  end

  @spec read(Engine.t(), Storable.offset(), integer, integer, integer) ::
          {:ok, any} | {:error, reason}
  def read(store, offset, uid, level, version \\ 1) do
    with {:ok,
          <<^version::10, ^level::6, ^uid::64, tag::8, size::16, checksum::32, buffer::binary>>} <-
           Engine.read(store, offset),
         {:ok, module} <- module(tag),
         {:ok, data} <- validata(buffer, size, checksum) do
      if level == 0 do
        module.load(data)
      else
        load_node(data, size, level, module)
      end
    else
      {:ok, _} -> {:error, :retired}
      error -> error
    end
  end

  defp dump_node(%Node{links: [], previous: previous}) do
    {:ok, 0, <<0::16, previous::64>>}
  end

  defp dump_node(%Node{links: links = [%Link{metadata: %kind{}} | _], previous: previous}) do
    with {:ok, tag} <- tag(kind),
         {:ok, iolist} <- dump_node(links, kind, 0, previous, []) do
      {:ok, tag, iolist}
    end
  end

  defp dump_node([], _kind, length, previous, iolist) do
    {:ok, [<<length::16, previous::64>> | iolist]}
  end

  defp dump_node([link | links], kind, length, previous, iolist) do
    iolist = [<<link.offset::64, kind.dump(link.metadata)::binary>> | iolist]
    dump_node(links, kind, length + 1, previous, iolist)
  end

  defp load_node(<<length::16, previous::64, buffer::binary>>, size, level, module) do
    metadata_size = div(size - 10, length) - 8
    load_node(buffer, metadata_size, module, level, previous, [])
  end

  defp load_node(<<offset::64, buffer::binary>>, size, module, level, previous, links) do
    with {:ok, metadata, buffer} <- take(buffer, size),
         {:ok, metadata} <- module.load(metadata) do
      load_node(buffer, size, module, level, previous, [
        %Link{offset: offset, metadata: metadata} | links
      ])
    end
  end

  defp load_node(<<>>, _size, _module, level, previous, links) do
    {:ok, %Node{level: level, previous: previous, links: links}}
  end

  defp load_node(_buffer, _size, _module, _level, _previous, _links) do
    {:error, :partial_data}
  end

  defp take(buffer, size) do
    case buffer do
      <<data::binary-size(size), buffer::binary>> ->
        {:ok, data, buffer}

      _ ->
        {:error, :partial_data}
    end
  end

  defp validata(buffer, size, checksum) do
    with {:ok, data, _} <- take(buffer, size) do
      if :erlang.crc32(data) == checksum do
        {:ok, data}
      else
        {:error, :corrupted}
      end
    end
  end

  defp tag(kind) do
    case Map.fetch(@tags, kind) do
      :error -> {:error, :unkown_tag}
      ok -> ok
    end
  end

  defp module(tag) do
    case Map.fetch(@modules, tag) do
      :error -> {:error, :unkown_tag}
      ok -> ok
    end
  end
end
