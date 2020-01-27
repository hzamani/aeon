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

    assert {:ok, metadata} = Series.metadata(series)
    assert {10, count * 10, count} == Aggregator.info(metadata)
    assert count * (count + 1) / 2 == metadata.sum
    assert {:ok, points} = Series.points(series)
    assert count == length(points)

    points
    |> Enum.with_index(1)
    |> Enum.each(fn {{timestamp, value}, i} ->
      assert timestamp == i * 10
      assert value == i
    end)
  end
end
