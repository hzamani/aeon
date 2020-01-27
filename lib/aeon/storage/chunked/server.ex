defmodule Aeon.Storage.Chunked.Server do
  defstruct [:file, :chunk, :free]

  use GenServer

  @impl GenServer
  def init(options) do
    path = Keyword.get(options, :path, "data.db")
    free = Keyword.get(options, :free, 1)
    chunk = Keyword.get(options, :chunk, 4096)

    case File.open(path, [:read, :write, :binary]) do
      {:ok, file} ->
        {:ok, %__MODULE__{file: file, chunk: chunk, free: free}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:write, data}, _client, state = %{free: free, chunk: chunk}) do
    chunks = div(IO.iodata_length(data) + chunk - 1, chunk)

    case :file.pwrite(state.file, free * chunk, data) do
      :ok -> {:reply, {:ok, free}, %{state | free: free + chunks}}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:read, address}, _client, state = %{chunk: chunk}) do
    result = :file.pread(state.file, address * chunk, chunk)
    {:reply, result, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    File.close(state.file)
  end
end
