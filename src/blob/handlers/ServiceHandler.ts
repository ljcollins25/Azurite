import BlobStorageContext from "../context/BlobStorageContext";
import * as Models from "../generated/artifacts/models";
import Context from "../generated/Context";
import IServiceHandler from "../generated/handlers/IServiceHandler";
import { parseXML } from "../generated/utils/xml";
import { BLOB_API_VERSION } from "../utils/constants";
import { getContainerGetAccountInfoResponse } from "../utils/utils";
import BaseHandler from "./BaseHandler";

/**
 * ServiceHandler handles Azure Storage Blob service related requests.
 *
 * @export
 * @class ServiceHandler
 * @implements {IHandler}
 */
export default class ServiceHandler extends BaseHandler
  implements IServiceHandler {
  /**
   * Default listing containers max number.
   *
   * @private
   * @memberof ServiceHandler
   */
  private readonly LIST_CONTAINERS_MAX_RESULTS_DEFAULT = 2000;

  /**
   * Default service properties.
   *
   * @private
   * @memberof ServiceHandler
   */
  private readonly defaultServiceProperties = {
    cors: [],
    defaultServiceVersion: BLOB_API_VERSION,
    hourMetrics: {
      enabled: false,
      retentionPolicy: {
        enabled: false
      },
      version: "1.0"
    },
    logging: {
      deleteProperty: true,
      read: true,
      retentionPolicy: {
        enabled: false
      },
      version: "1.0",
      write: true
    },
    minuteMetrics: {
      enabled: false,
      retentionPolicy: {
        enabled: false
      },
      version: "1.0"
    },
    staticWebsite: {
      enabled: false
    }
  };

  public getUserDelegationKey(
    keyInfo: Models.KeyInfo,
    options: Models.ServiceGetUserDelegationKeyOptionalParams,
    context: Context
  ): Promise<Models.ServiceGetUserDelegationKeyResponse> {
    throw new Error("Method not implemented.");
  }
  public submitBatch(
    body: NodeJS.ReadableStream,
    contentLength: number,
    multipartContentType: string,
    options: Models.ServiceSubmitBatchOptionalParams,
    context: Context
  ): Promise<Models.ServiceSubmitBatchResponse> {
    throw new Error("Method not implemented.");
  }

  /**
   * Set blob service properties.
   *
   * @param {Models.StorageServiceProperties} storageServiceProperties
   * @param {Models.ServiceSetPropertiesOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ServiceSetPropertiesResponse>}
   * @memberof ServiceHandler
   */
  public async setProperties(
    storageServiceProperties: Models.StorageServiceProperties,
    options: Models.ServiceSetPropertiesOptionalParams,
    context: Context
  ): Promise<Models.ServiceSetPropertiesResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;

    // TODO: deserializor has a bug that when cors is undefined,
    // it will serialize it to empty array instead of undefined
    const body = blobCtx.request!.getBody();
    const parsedBody = await parseXML(body || "");
    if (
      !parsedBody.hasOwnProperty("cors") &&
      !parsedBody.hasOwnProperty("Cors")
    ) {
      storageServiceProperties.cors = undefined;
    }

    await this.metadataStore.setServiceProperties({
      ...storageServiceProperties,
      accountName
    });

    const response: Models.ServiceSetPropertiesResponse = {
      requestId: context.contextID,
      statusCode: 202,
      version: BLOB_API_VERSION
    };
    return response;
  }

  /**
   * Get blob service properties.
   *
   * @param {Models.ServiceGetPropertiesOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ServiceGetPropertiesResponse>}
   * @memberof ServiceHandler
   */
  public async getProperties(
    options: Models.ServiceGetPropertiesOptionalParams,
    context: Context
  ): Promise<Models.ServiceGetPropertiesResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;

    let properties = await this.metadataStore.getServiceProperties(accountName);
    if (!properties) {
      properties = { ...this.defaultServiceProperties, accountName };
    }

    if (properties.cors === undefined) {
      properties.cors = [];
    }

    if (properties.cors === undefined) {
      properties.cors = [];
    }

    if (properties.hourMetrics === undefined) {
      properties.hourMetrics = this.defaultServiceProperties.hourMetrics;
    }

    if (properties.logging === undefined) {
      properties.logging = this.defaultServiceProperties.logging;
    }

    if (properties.minuteMetrics === undefined) {
      properties.minuteMetrics = this.defaultServiceProperties.minuteMetrics;
    }

    if (properties.defaultServiceVersion === undefined) {
      properties.defaultServiceVersion = this.defaultServiceProperties.defaultServiceVersion;
    }

    if (properties.staticWebsite === undefined) {
      properties.staticWebsite = this.defaultServiceProperties.staticWebsite;
    }

    const response: Models.ServiceGetPropertiesResponse = {
      ...properties,
      requestId: context.contextID,
      statusCode: 200,
      version: BLOB_API_VERSION
    };
    return response;
  }

  public async getStatistics(
    options: Models.ServiceGetStatisticsOptionalParams,
    context: Context
  ): Promise<Models.ServiceGetStatisticsResponse> {
    const response: Models.ServiceGetStatisticsResponse = {
      statusCode: 200,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      date: context.startTime,
      geoReplication: {
        status: Models.GeoReplicationStatusType.Live,
        lastSyncTime: context.startTime!
      }
    };
    return response;
  }

  /**
   * List containers.
   *
   * @param {Models.ServiceListContainersSegmentOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ServiceListContainersSegmentResponse>}
   * @memberof ServiceHandler
   */
  public async listContainersSegment(
    options: Models.ServiceListContainersSegmentOptionalParams,
    context: Context
  ): Promise<Models.ServiceListContainersSegmentResponse> {
    const blobCtx = new BlobStorageContext(context);
    const request = blobCtx.request!;
    const accountName = blobCtx.account!;

    options.maxresults =
      options.maxresults || this.LIST_CONTAINERS_MAX_RESULTS_DEFAULT;
    options.prefix = options.prefix || "";

    const marker = parseInt(options.marker || "0", 10);

    const containers = await this.metadataStore.listContainers(
      accountName,
      options.prefix,
      options.maxresults,
      marker
    );

    // TODO: Need update list out container lease properties with ContainerHandler.updateLeaseAttributes()
    const serviceEndpoint = `${request.getEndpoint()}/${accountName}`;
    const res: Models.ServiceListContainersSegmentResponse = {
      containerItems: containers[0],
      maxResults: options.maxresults,
      nextMarker: `${containers[1] || ""}`,
      prefix: options.prefix,
      serviceEndpoint,
      statusCode: 200,
      requestId: context.contextID,
      version: BLOB_API_VERSION
    };

    return res;
  }

  public async getAccountInfo(
    context: Context
  ): Promise<Models.ServiceGetAccountInfoResponse> {
    return getContainerGetAccountInfoResponse(context);
  }

  public async getAccountInfoWithHead(
    context: Context
  ): Promise<Models.ServiceGetAccountInfoResponse> {
    return getContainerGetAccountInfoResponse(context);
  }
}