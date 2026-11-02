
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

const string queueName = "pedidos";

// La API se conecta una vez y mantiene la conexión abierta.
var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST")
               ?? "localhost",
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER")
               ?? "admin",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD")
               ?? "admin"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// Declarar una cola durable.
await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

// Serializar las publicaciones para no publicar simultáneamente
// por el mismo canal.
var publishLock = new SemaphoreSlim(1, 1);

app.MapGet("/", () =>
{
    return Results.Ok(new
    {
        status = "running",
        service = "orders-api",
        rabbitMq = "connected"
    });
});

app.MapPost("/orders", async (
    CreateOrderRequest request,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerName) ||
        string.IsNullOrWhiteSpace(request.Product) ||
        request.Quantity <= 0)
    {
        return Results.BadRequest(new
        {
            status = "error",
            message = "Los datos del pedido no son válidos."
        });
    }

    var orderId = $"ORD-{Guid.NewGuid():N}";

    var message = new
    {
        type = "ProcessOrder",
        orderId,
        customerName = request.CustomerName,
        product = request.Product,
        quantity = request.Quantity,
        createdAt = DateTimeOffset.UtcNow
    };

    var body = Encoding.UTF8.GetBytes(
        JsonSerializer.Serialize(message)
    );

    var properties = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        MessageId = orderId
    };

    try
    {
        await publishLock.WaitAsync(cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken
            );
        }
        finally
        {
            publishLock.Release();
        }

        app.Logger.LogInformation(
            "Pedido {OrderId} publicado en RabbitMQ",
            orderId
        );

        return Results.Accepted($"/orders/{orderId}", new
        {
            status = "accepted",
            message = "Pedido recibido y enviado a RabbitMQ",
            orderId
        });
    }
    catch (Exception ex)
    {
        app.Logger.LogError(
            ex,
            "Error publicando el pedido {OrderId}",
            orderId
        );

        return Results.Problem(
            title: "No fue posible publicar el pedido en RabbitMQ",
            statusCode: StatusCodes.Status503ServiceUnavailable
        );
    }
});

app.Run();

public record CreateOrderRequest(
    string CustomerName,
    string Product,
    int Quantity
);
