defmodule Aeon.Aggregation.Float do
  defstruct [:count, :from, :to, :min, :max, :sum]

  use Aeon.Storage.Storable

  def dump(meta) do
    <<meta.count::16, meta.from::64, meta.to::64, meta.min::float-64, meta.max::float-64,
      meta.sum::float-64>>
  end

  def load(<<count::16, from::64, to::64, min::float-64, max::float-64, sum::float-64>>) do
    meta = %__MODULE__{
      count: count,
      from: from,
      to: to,
      min: min,
      max: max,
      sum: sum
    }

    {:ok, meta}
  end

  defimpl Aeon.Aggregator do
    alias Aeon.Aggregation.Float

    def clear(_), do: %Float{count: 0}

    def insert(%{count: 0}, {timestamp, value}) do
      %Float{
        count: 1,
        from: timestamp,
        to: timestamp,
        min: value,
        max: value,
        sum: value
      }
    end

    def insert(meta, {timestamp, value}) do
      %{
        meta
        | count: meta.count + 1,
          to: timestamp,
          min: min(meta.min, value),
          max: max(meta.max, value),
          sum: meta.sum + value
      }
    end

    def merge(%{count: 0}, b), do: b
    def merge(a, %{count: 0}), do: a

    def merge(a, b) do
      %Float{
        count: a.count + b.count,
        from: min(a.from, b.from),
        to: max(a.to, b.to),
        min: min(a.min, b.min),
        max: max(a.max, b.max),
        sum: a.sum + b.sum
      }
    end

    def info(meta) do
      {meta.from, meta.to, meta.count}
    end
  end
end
