defmodule Aeon.Storage.Storable do
  @type t :: any
  @callback dump(t) :: iodata
  @callback load(iodata) :: {:ok, t} | {:error, atom}

  defmacro __using__(_options) do
    quote do
      alias Aeon.Storage.Storable
      @behaviour Aeon.Storage.Storable
    end
  end
end
