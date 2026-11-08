defmodule Jobcelis.FakeServer do
  @moduledoc false
  # Minimal HTTP server that answers each request with the next scripted response.
  # A response of :drop closes the connection without answering (a network error).

  def start(responses) do
    {:ok, listen} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :line,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start_link(fn -> %{responses: responses, requests: []} end)
    pid = spawn_link(fn -> accept_loop(listen, agent) end)
    :ok = :gen_tcp.controlling_process(listen, pid)

    %{url: "http://127.0.0.1:#{port}", agent: agent}
  end

  def requests(%{agent: agent}), do: Agent.get(agent, &Enum.reverse(&1.requests))

  defp accept_loop(listen, agent) do
    {:ok, socket} = :gen_tcp.accept(listen)
    handle(socket, agent)
    accept_loop(listen, agent)
  end

  defp handle(socket, agent) do
    {:ok, request_line} = :gen_tcp.recv(socket, 0)
    [method, path | _] = String.split(request_line, " ")
    headers = read_headers(socket, %{})

    body =
      case Map.get(headers, "content-length", "0") |> String.to_integer() do
        0 ->
          ""

        length ->
          :ok = :inet.setopts(socket, packet: :raw)
          {:ok, body} = :gen_tcp.recv(socket, length)
          body
      end

    request = %{method: method, path: path, headers: headers, body: body}

    response =
      Agent.get_and_update(agent, fn state ->
        {next, rest} =
          case state.responses do
            [next | rest] -> {next, rest}
            [] -> {%{status: 500, body: %{error: "no response scripted"}}, []}
          end

        {next, %{state | responses: rest, requests: [request | state.requests]}}
      end)

    reply(socket, response)
    :gen_tcp.close(socket)
  end

  defp read_headers(socket, acc) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, "\r\n"} ->
        acc

      {:ok, line} ->
        [name, value] = String.split(line, ":", parts: 2)
        read_headers(socket, Map.put(acc, String.downcase(name), String.trim(value)))
    end
  end

  defp reply(_socket, :drop), do: :ok

  defp reply(socket, response) do
    payload = if Map.has_key?(response, :body), do: Jason.encode!(response.body), else: ""

    extra =
      response
      |> Map.get(:headers, %{})
      |> Enum.map(fn {k, v} -> "#{k}: #{v}\r\n" end)

    :gen_tcp.send(socket, [
      "HTTP/1.1 #{response.status} X\r\ncontent-type: application/json\r\n",
      "content-length: #{byte_size(payload)}\r\nconnection: close\r\n",
      extra,
      "\r\n",
      payload
    ])
  end
end
