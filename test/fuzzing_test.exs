defmodule EflatbuffersTest.Fuzz do
  use ExUnit.Case
  import TestHelpers

  test "fuzz based on doge schemas" do
    #[:config, :game_state, :battle_log, :commands]
    [:commands]
    |> Enum.map(fn(type) -> {:doge, type} end)
    |> Enum.each(
      fn(doge_type) ->
        IO.inspect("Testing schema: #{inspect(doge_type)}")
        fuzz_schema(doge_type)
      end
    )
  end

  def fuzz_schema(schema_type) do
    map    = Eflatbuffers.Generator.map_from_schema(load_schema(schema_type))
IO.inspect {:map, map}
    fb     = Eflatbuffers.write!(map, load_schema(schema_type))
IO.inspect {:fb, fb}
    map_re = Eflatbuffers.read!(:erlang.iolist_to_binary(fb), load_schema(schema_type))
IO.inspect {:map_re, map_re}
    assert [] == compare_with_defaults(round_floats(map), round_floats(map_re), Eflatbuffers.parse_schema!(load_schema(schema_type)))

    assert_full_circle(schema_type, map)
  end

end
