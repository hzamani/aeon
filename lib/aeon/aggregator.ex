defprotocol Aeon.Aggregator do
  @type t :: Storage.Storable.t()
  @type t(item) :: item | t
  @type timestamp :: non_neg_integer
  @type count :: non_neg_integer

  @spec clear(t(item)) :: t(item) when item: var
  def clear(metadata)

  @spec insert(t(item), item) :: t(item) when item: var
  def insert(metadata, item)

  @spec merge(t(item), t(item)) :: t(item) when item: var
  def merge(metadata, other)

  @spec info(t) :: {timestamp, timestamp, count} when item: var
  def info(metadata)
end
