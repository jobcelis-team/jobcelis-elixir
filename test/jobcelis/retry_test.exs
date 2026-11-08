defmodule Jobcelis.RetryTest do
  use ExUnit.Case, async: true

  alias Jobcelis.FakeServer

  @uuid ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  defp setup_client(responses, opts \\ []) do
    server = FakeServer.start(responses)
    client = Jobcelis.client("key", [base_url: server.url, retry_base_delay: 1] ++ opts)
    {client, server}
  end

  defp ok(body \\ %{"ok" => true}), do: %{status: 200, body: body}

  test "GET is retried on 503 until it succeeds" do
    {client, server} = setup_client([%{status: 503}, %{status: 503}, ok(%{"id" => "evt_1"})])

    assert {:ok, %{"id" => "evt_1"}} = Jobcelis.get_event(client, "evt_1")
    assert length(FakeServer.requests(server)) == 3
  end

  test "429 with Retry-After is retried" do
    {client, server} = setup_client([%{status: 429, headers: %{"retry-after" => "0"}}, ok()])

    assert {:ok, _} = Jobcelis.list_webhooks(client)
    assert length(FakeServer.requests(server)) == 2
  end

  test "Retry-After as an HTTP date in the past is retried right away" do
    date = "Wed, 21 Oct 2015 07:28:00 GMT"
    {client, server} = setup_client([%{status: 503, headers: %{"retry-after" => date}}, ok()])

    assert {:ok, _} = Jobcelis.get_event(client, "evt_1")
    assert length(FakeServer.requests(server)) == 2
  end

  test "send_event sends the same generated X-Idempotency-Key on every attempt" do
    {client, server} = setup_client([%{status: 502}, :drop, ok()])

    assert {:ok, _} = Jobcelis.send_event(client, "order.created", %{id: 1})

    keys = Enum.map(FakeServer.requests(server), & &1.headers["x-idempotency-key"])
    assert length(keys) == 3
    assert hd(keys) =~ @uuid
    assert Enum.uniq(keys) == [hd(keys)]
  end

  test "send_event keeps a caller-supplied idempotency key" do
    {client, server} = setup_client([%{status: 503}, ok()])

    assert {:ok, _} =
             Jobcelis.send_event(client, "order.created", %{id: 1}, idempotency_key: "order-1")

    assert Enum.map(FakeServer.requests(server), & &1.headers["x-idempotency-key"]) ==
             ["order-1", "order-1"]
  end

  test "send_events adds missing idempotency keys and keeps existing ones" do
    {client, server} = setup_client([%{status: 503}, ok()])

    events = [
      %{topic: "a", payload: %{}},
      %{topic: "b", payload: %{}, idempotency_key: "mine"}
    ]

    assert {:ok, _} = Jobcelis.send_events(client, events)

    [first, second] =
      Enum.map(FakeServer.requests(server), &(Jason.decode!(&1.body) |> Map.fetch!("events")))

    assert first == second
    assert Enum.at(first, 0)["idempotency_key"] =~ @uuid
    assert Enum.at(first, 1)["idempotency_key"] == "mine"
  end

  test "POST without an idempotency key is not retried on a network error" do
    {client, server} = setup_client([:drop, ok()])

    assert {:error, %Jobcelis.Error{status: 0}} =
             Jobcelis.create_webhook(client, "https://example.com")

    assert length(FakeServer.requests(server)) == 1
  end

  test "POST without an idempotency key is retried on 503" do
    {client, server} = setup_client([%{status: 503}, ok()])

    assert {:ok, _} = Jobcelis.create_webhook(client, "https://example.com")
    assert length(FakeServer.requests(server)) == 2
  end

  test "GET is retried on a network error" do
    {client, server} = setup_client([:drop, ok()])

    assert {:ok, _} = Jobcelis.get_event(client, "evt_1")
    assert length(FakeServer.requests(server)) == 2
  end

  test "max_retries: 0 makes exactly one request" do
    {client, server} = setup_client([%{status: 503}, ok()], max_retries: 0)

    assert {:error, %Jobcelis.Error{status: 503}} = Jobcelis.get_event(client, "evt_1")
    assert length(FakeServer.requests(server)) == 1
  end

  test "returns the usual error after the last retry" do
    responses = List.duplicate(%{status: 503, body: %{error: "down"}}, 4)
    {client, server} = setup_client(responses, max_retries: 3)

    assert {:error, %Jobcelis.Error{status: 503, detail: %{"error" => "down"}}} =
             Jobcelis.get_event(client, "evt_1")

    assert length(FakeServer.requests(server)) == 4
  end

  for status <- [400, 500] do
    test "status #{status} is not retried" do
      {client, server} = setup_client([%{status: unquote(status), body: %{error: "no"}}, ok()])

      assert {:error, %Jobcelis.Error{status: unquote(status)}} =
               Jobcelis.get_event(client, "evt_1")

      assert length(FakeServer.requests(server)) == 1
    end
  end

  test "defaults to 6 retries with a 1 s base delay" do
    client = Jobcelis.client("key")
    assert client.max_retries == 6
    assert client.retry_base_delay == 1_000
  end
end
