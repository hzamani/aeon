defprotocol Aeon.Storage.Engine do
  @type t :: any
  @type offset :: non_neg_integer

  @spec write(t, iodata) :: {:ok, offset} | {:error, File.posix()}
  def write(storage, iodata)

  @spec read(t, offset) :: {:ok, iodata} | {:error, File.posix()}
  def read(storage, offset)
end
