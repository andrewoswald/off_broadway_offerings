defmodule Offerings.StreamHandler do
  @gzip_window_bits 31
  @gzip_eos_behavior :reset
  @prefix_bit_len 32

  import :zlib, only: [safeInflate: 2]
  import IO, only: [iodata_to_binary: 1]
  import Bitwise

  def init(msg_handler) do
    zstream = :zlib.open()
    :zlib.inflateInit(zstream, @gzip_window_bits, @gzip_eos_behavior)
    &prefix(zstream, msg_handler, safeInflate(zstream, &1))
  end

  defp prefix(zstream, msg_handler, {tag, data}, needs \\ @prefix_bit_len, acc_len \\ 0) do
    case data do
      [<<len::size(needs), payload::binary>>] -> payload(zstream, msg_handler, acc_len + len, {tag, [payload]})
      _ ->
        prefix_part = List.first(data, <<>>)
        new_needs = needs - bit_size(prefix_part)
        new_acc_len = acc_len + (:binary.decode_unsigned(prefix_part) <<< new_needs)
        case tag do
          :continue -> prefix(zstream, msg_handler, safeInflate(zstream, []), new_needs, new_acc_len)
          :finished -> &prefix(zstream, msg_handler, safeInflate(zstream, &1), new_needs, new_acc_len)
        end
    end
  end

  defp payload(zstream, msg_handler, needs, {tag, data}, acc \\ []) do
    case data do
      [<<val::binary-size(needs)>>] ->
        [acc, val] |> iodata_to_binary |> msg_handler.()
        case tag do
          :continue -> prefix(zstream, msg_handler, safeInflate(zstream, []))
          :finished -> &prefix(zstream, msg_handler, safeInflate(zstream, &1))
        end
      [<<val::binary-size(needs), next_frame::binary>>] ->
        [acc, val] |> iodata_to_binary |> msg_handler.()
        prefix(zstream, msg_handler, {tag, [next_frame]})
      _ ->
        new_needs = needs - byte_size(List.first(data, <<>>))
        case tag do
          :continue -> payload(zstream, msg_handler, new_needs, safeInflate(zstream, []), [acc, data])
          :finished -> &payload(zstream, msg_handler, new_needs, safeInflate(zstream, &1), [acc, data])
        end
    end
  end
end
