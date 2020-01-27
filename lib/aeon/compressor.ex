defprotocol Aeon.Compressor do
  @type t :: Aeon.Storage.Storable.t()
  @type t(item) :: item | t
  @type stream(item) :: [item] | Enumerable.t()

  @spec capacity(t) :: pos_integer
  def capacity(compressor)

  @spec clear(t(item)) :: t(item) when item: var
  def clear(compressor)

  @spec append(t(item), item) :: {:ok, t(item)} | :overflow when item: var
  def append(compressor, item)

  @spec stream(t(item)) :: stream(item) when item: var
  def stream(compressor)
end
