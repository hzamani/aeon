defmodule Aeon.Compression.None do
  defstruct buffer: <<>>, capacity: 4096

  use Aeon.Storage.Storable

  def dump(none), do: none.buffer

  def load(buffer), do: {:ok, %__MODULE__{buffer: buffer, capacity: byte_size(buffer)}}

  defimpl Aeon.Compressor do
    def capacity(none), do: none.capacity
    def clear(none), do: %{none | buffer: <<>>}

    def append(none, {timestamp, value}) do
      if byte_size(none.buffer) <= none.capacity - 16 do
        {:ok, %{none | buffer: none.buffer <> <<timestamp::64, value::float-64>>}}
      else
        :overflow
      end
    end

    def stream(none) do
      Stream.unfold(none.buffer, fn
        <<timestamp::64, value::float-64, buffer::binary>> -> {{timestamp, value}, buffer}
        _ -> nil
      end)
    end
  end
end
