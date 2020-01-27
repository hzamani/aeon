defmodule Aeon.Tree.Link do
  defstruct [:metadata, :offset]

  alias Aeon.Aggregator

  @type t :: %__MODULE__{metadata: Aggregator.t(), offset: pos_integer}
end
