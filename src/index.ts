import {
    AttributeValue,
    DynamoDBClient,
    ReturnValue,
} from '@aws-sdk/client-dynamodb';
import {
    BatchGetCommand,
    BatchWriteCommand,
    DynamoDBDocumentClient,
    GetCommand,
    PutCommand,
    QueryCommand,
    UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { v4 as uuid } from 'uuid';
import { ErrorMessage } from './constants/enums/error.message/errorMessage';


export class DatabaseService {
  private docClient: DynamoDBDocumentClient;

  constructor(region: string) {
    const client = new DynamoDBClient({ region });
    this.docClient = DynamoDBDocumentClient.from(client);
  }

  async getItem<T>(params: {
    table: string;
    key: { [subKey: string]: string };
  }): Promise<T> {
    const command = new GetCommand({
      TableName: params.table,
      Key: params.key,
    });
    const response = await this.docClient.send(command).catch((error) => {
      if (error.message === 'Requested resource not found')
        throw new Error(ErrorMessage.NOT_FOUND);
      throw error;
    });

    if (response.Item === undefined) throw new Error(ErrorMessage.NOT_FOUND);

    return response.Item as T;
  }

  async batchGetItems(params: {
    ids: string[];
    table: string;
  }): Promise<Record<string, AttributeValue>[]> {
    const command = new BatchGetCommand({
      RequestItems: {
        [params.table]: {
          Keys: params.ids.map((id) => {
            return { id: id };
          }),
        },
      },
    });

    const response = await this.docClient.send(command);
    return response?.Responses?.[params.table] ?? [];
  }

  async listItemsByIndex<T>(params: {
    index: string;
    values: Record<string, string>;
    table: string;
  }): Promise<T[]> {
    const attributeValues: Record<string, string> = {};
    Object.keys(params.values).forEach((key) => {
      attributeValues[`:${key}`] = params.values[key];
    });

    const command = new QueryCommand({
      KeyConditionExpression: Object.keys(params.values)
        .map((val) => `${val} = :${val}`)
        .join(' AND '),
      ExpressionAttributeValues: attributeValues,
      IndexName: params.index,
      TableName: params.table,
    });

    const response = (
      await this.docClient.send(command).catch((error) => {
        throw error;
      })
    ).Items?.map((item) => {
      return item as T;
    });

    if (response?.length === 0) throw new Error(ErrorMessage.NOT_FOUND);

    return response ?? [];
  }

  async createItem<T>(params: {
    data: T;
    table: string;
  }): Promise<T> {
    const item = this.addTimestamps(this.generateID(params.data));

    const command = new PutCommand({
      TableName: params.table,
      Item: item as Record<string, any>,
      ReturnValues: ReturnValue.ALL_OLD,
    });

    await this.docClient.send(command).catch((error) => {
      throw error;
    });

    return item as T;
  }

  async batchCreateItems<T>(params: {
    items: T[];
    table: string;
  }): Promise<T[]> {
    const batches = this.createBatches(params.items, 25);

    const response: T[] = [];

    for (const batch of batches) {
      const putRequests = batch.map((item) => {
        const requestItem = this.addTimestamps(this.generateID(item)) as Record<string, any>;

        response.push(requestItem as T);
        return {
          PutRequest: {
            Item: requestItem,
            ReturnValues: ReturnValue.ALL_OLD,
          },
        };
      });

      console.log('!!INFO: Request', batch);

      const command = new BatchWriteCommand({
        RequestItems: {
          [params.table]: putRequests,
        },
      });

      await this.docClient.send(command).catch((error) => {
        throw error;
      });
    }

    return response as T[];
  }

  async updateItem<T>(params: {
    key: { [subKey: string]: string };
    table: string;
    data: Record<string, any>;
  }): Promise<T> {
    params.data = this.modifyUpdatedAtTimestamp(params.data);

    const updateExpression =
      'set ' +
      Object.keys(params.data)
        .map((key) => `#${key} = :${key}`)
        .join(', ');

    const expressionAttributeNames: Record<string, string> = {};
    Object.keys(params.data).forEach((key) => {
      expressionAttributeNames[`#${key}`] = key;
    });

    const attributeValues: Record<string, any> = {};
    Object.keys(params.data).forEach((key) => {
      attributeValues[`:${key}`] = params.data[key];
    });

    const command = new UpdateCommand({
      TableName: params.table,
      Key: params.key,
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: attributeValues,
      ReturnValues: ReturnValue.ALL_NEW,
    });

    const response = await this.docClient.send(command).catch((error) => {
      throw error;
    });

    return response.Attributes as T;
  }

  async addItems(params: {
    table: string;
    items: Record<string, any>[];
  }): Promise<void> {
    // chunkArray is a local convenience function. It takes an array and returns
    // a generator function. The generator function yields every N items.
    const chunks = this.createBatch(params.items, 25);

    // For every chunk of 25 movies, make one BatchWrite request.
    for (const chunk of chunks) {
      const putRequests = chunk.map((movie) => ({
        PutRequest: {
          Item: this.addCreatedAt(movie),
        },
      }));

      console.log('request', chunk);

      const command = new BatchWriteCommand({
        RequestItems: {
          [params.table]: putRequests,
        },
      });

      const res = await this.docClient.send(command);
      console.log('res', res);
    }
  }

  async addItem(params: {
    table: string;
    data: Record<string, any>;
    excludeCreatedAt?: boolean;
  }): Promise<void> {
    const excludeCreatedAt = params.excludeCreatedAt ?? false;
    const item = excludeCreatedAt
      ? params.data
      : this.addCreatedAt(params.data);
    const command = new PutCommand({
      TableName: params.table,
      Item: item,
    });

    const response = await this.docClient.send(command);
    console.log('addItem', response);
  }

  private addTimestamps<T>(data: T): T {
    const item = { ...data } as Record<string, any>;
  
    item['createdAt'] = Date.now();
    item['updatedAt'] = Date.now();
  
    return item as T;
  }

  private modifyUpdatedAtTimestamp<T>(data: Record<string, any>): T {
    const item = { ...data };

    item['updatedAt'] = Date.now();

    return item as T;
  }


  private createBatches<T>(items: T[], batchSize = 25): T[][] {
    const batches: T[][] = [];

    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      batches.push(batch as T[]);
    }
    return batches;
  }

  private generateID<T>(data: T): T {
    const newData = { ...data } as Record<string, any>;
  
    if (!('id' in newData) || newData['id'] === undefined || newData['id'] === null) {
      newData['id'] = uuid();
    }
  
    return newData as T;
  }

  private addCreatedAt(data: Record<string, any>): Record<string, any> {
    return {
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      ...data,
    };
  }

  private createBatch<T>(items: T[], batchSize = 25): T[][] {
    const batches = [];
    for (let i = 0; i < items.length; i += batchSize) {
      const chunk = items.slice(i, i + batchSize);
      batches.push(chunk);
    }
    return batches;
  }
}
