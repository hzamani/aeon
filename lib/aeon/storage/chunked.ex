defmodule Aeon.Storage.Chunked do
  defstruct [:server]

  alias Aeon.Storage.Chunked.Server

  def start_link(path, chunk \\ 4096) do
    with {:ok, size} <- file_size(path),
         {:ok, pid} <-
           GenServer.start_link(Server,
             path: path,
             chunk: chunk,
             free: div(size + chunk - 1, chunk) + 1
           ) do
      {:ok, %__MODULE__{server: pid}}
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %File.Stat{size: size}} -> {:ok, size}
      {:error, :enoent} -> {:ok, 0}
      error -> error
    end
  end

  defimpl Aeon.Storage.Engine do
    def write(%{server: server}, iodata) do
      GenServer.call(server, {:write, iodata})
    end

    def read(%{server: server}, offset) do
      GenServer.call(server, {:read, offset})
    end
  end
end
