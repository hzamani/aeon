defmodule AeonTest do
  use ExUnit.Case
  doctest Aeon

  alias Aeon.{Series, Aggregator}
  alias Aeon.Storage.Chunked
  alias Aeon.Compression.None
  alias Aeon.Aggregation.Float

  test "series" do
    capacity = 240
    assert {:ok, store} = Chunked.start_link("data.db", capacity)
    count = 200
    series = Series.new(1, %None{capacity: capacity - 17}, %Float{count: 0}, store, 4)

    series =
      Enum.reduce(1..count, series, fn i, series ->
        assert {:ok, series} = Series.append(series, {i * 10, i})
        series
      end)

    assert {:ok, metadata} = Series.metadata(series, 0)
    assert {10, count * 10, count} == Aggregator.info(metadata)
    assert count * (count + 1) / 2 == metadata.sum

    assert {:ok, points} = Series.points(series, 0)
    assert count == length(points)

    points
    |> Enum.with_index(1)
    |> Enum.each(fn {{timestamp, value}, i} ->
      assert timestamp == i * 10
      assert value == i
    end)

    take = 100
    from = (count - take) * 10
    assert {:ok, points} = Series.points(series, from)
    assert take + 1 == length(points)

    assert {:ok, metadata} = Series.metadata(series, from)
    assert {from, count * 10, take + 1} == Aggregator.info(metadata)
    assert count * (take + 1) - take * (take + 1) / 2 == metadata.sum
  end
end
