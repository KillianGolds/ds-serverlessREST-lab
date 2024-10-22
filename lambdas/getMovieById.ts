import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));
    
    // Extract path parameters and query string parameters
    const parameters = event?.pathParameters;
    const queryParams = event?.queryStringParameters;
    
    const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;
    const includeCast = queryParams?.cast === "true"; // Check if cast=true query parameter is present

    if (!movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Missing movie Id" }),
      };
    }

    // Fetch movie metadata from the movies table
    const commandOutput = await ddbDocClient.send(
      new GetCommand({
        TableName: process.env.TABLE_NAME,
        Key: { id: movieId },
      })
    );

    console.log("GetCommand response: ", commandOutput);

    if (!commandOutput.Item) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Invalid movie Id" }),
      };
    }

    const body: any = {
      data: commandOutput.Item,
    };


    // Check if the query parameter ?cast=true is present
    // If present retrieve the cast details from the MovieCast table
    if (includeCast) {
      const castCommand = new QueryCommand({
        TableName: process.env.CAST_TABLE_NAME, // MovieCast table name
        KeyConditionExpression: "movieId = :movieId",
        ExpressionAttributeValues: {
          ":movieId": movieId, // Movie Id from the cast data
        },
      });

      // Fetch cast details from the MovieCast table
      const castResponse = await ddbDocClient.send(castCommand);

      console.log("Cast Query response: ", castResponse);
      
      // Add the cast data to the response body
      body.cast = castResponse.Items || [];
    }

    // Return the response with movie metadata and cast details
    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    };

  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error: error.message }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}