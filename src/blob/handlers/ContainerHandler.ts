import BlobStorageContext from "../context/BlobStorageContext";
import * as Models from "../generated/artifacts/models";
import Context from "../generated/Context";
import IContainerHandler from "../generated/handlers/IContainerHandler";
import { BLOB_API_VERSION } from "../utils/constants";
import { getContainerGetAccountInfoResponse, newEtag } from "../utils/utils";
import BaseHandler from "./BaseHandler";

/**
 * ContainerHandler handles Azure Storage Blob container related requests.
 *
 * @export
 * @class ContainerHandler
 * @implements {IHandler}
 */
export default class ContainerHandler extends BaseHandler
  implements IContainerHandler {
  /**
   * Default listing blobs max number.
   *
   * @private
   * @memberof ContainerHandler
   */
  private readonly LIST_BLOBS_MAX_RESULTS_DEFAULT = 5000;

  /**
   * create container
   *
   * @param {Models.ContainerCreateOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerCreateResponse>}
   * @memberof ContainerHandler
   */
  public async create(
    options: Models.ContainerCreateOptionalParams,
    context: Context
  ): Promise<Models.ContainerCreateResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;
    const lastModified = blobCtx.startTime!;
    const etag = newEtag();

    await this.metadataStore.createContainer(
      {
        accountName,
        metadata: options.metadata,
        name: containerName,
        properties: {
          etag,
          lastModified,
          leaseStatus: Models.LeaseStatusType.Unlocked,
          leaseState: Models.LeaseStateType.Available,
          publicAccess: options.access,
          hasImmutabilityPolicy: false,
          hasLegalHold: false
        }
      },
      context
    );

    const response: Models.ContainerCreateResponse = {
      eTag: etag,
      lastModified,
      requestId: blobCtx.contextID,
      statusCode: 201,
      version: BLOB_API_VERSION
    };

    return response;
  }

  /**
   * get container properties
   *
   * @param {Models.ContainerGetPropertiesOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerGetPropertiesResponse>}
   * @memberof ContainerHandler
   */
  public async getProperties(
    options: Models.ContainerGetPropertiesOptionalParams,
    context: Context
  ): Promise<Models.ContainerGetPropertiesResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;
    const containerProperties = await this.metadataStore.getContainerProperties(
      accountName,
      containerName,
      context
    );

    const response: Models.ContainerGetPropertiesResponse = {
      eTag: containerProperties.properties.etag,
      ...containerProperties.properties,
      blobPublicAccess: containerProperties.properties.publicAccess,
      metadata: containerProperties.metadata,
      requestId: context.contextID,
      statusCode: 200,
      version: BLOB_API_VERSION
    };
    return response;
  }

  /**
   * get container properties with headers
   *
   * @param {Models.ContainerGetPropertiesOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerGetPropertiesResponse>}
   * @memberof ContainerHandler
   */
  public async getPropertiesWithHead(
    options: Models.ContainerGetPropertiesOptionalParams,
    context: Context
  ): Promise<Models.ContainerGetPropertiesResponse> {
    return this.getProperties(options, context);
  }

  /**
   * delete container
   *
   * @param {Models.ContainerDeleteMethodOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerDeleteResponse>}
   * @memberof ContainerHandler
   */
  public async delete(
    options: Models.ContainerDeleteMethodOptionalParams,
    context: Context
  ): Promise<Models.ContainerDeleteResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    // TODO: Mark container as being deleted status, then (mark) delete all blobs async
    // When above finishes, execute following delete container operation
    // Because following delete container operation will only delete DB metadata for container and
    // blobs under the container, but will not clean up blob data in disk
    // The current design will directly remove the container and all the blobs belong to it.
    await this.metadataStore.deleteContainer(
      accountName,
      containerName,
      options.leaseAccessConditions,
      context
    );

    const response: Models.ContainerDeleteResponse = {
      date: context.startTime,
      requestId: context.contextID,
      statusCode: 202,
      version: BLOB_API_VERSION
    };

    return response;
  }

  /**
   * set container metadata
   *
   * @param {Models.ContainerSetMetadataOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerSetMetadataResponse>}
   * @memberof ContainerHandler
   */
  public async setMetadata(
    options: Models.ContainerSetMetadataOptionalParams,
    context: Context
  ): Promise<Models.ContainerSetMetadataResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const date = blobCtx.startTime!;
    const eTag = newEtag();
    await this.metadataStore.setContainerMetadata(
      accountName,
      containerName,
      date,
      eTag,
      options.metadata,
      context
    );

    const response: Models.ContainerSetMetadataResponse = {
      date,
      eTag,
      lastModified: date,
      requestId: context.contextID,
      statusCode: 200
    };

    return response;
  }

  /**
   * Get container access policy
   *
   * @param {Models.ContainerGetAccessPolicyOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerGetAccessPolicyResponse>}
   * @memberof ContainerHandler
   */
  public async getAccessPolicy(
    options: Models.ContainerGetAccessPolicyOptionalParams,
    context: Context
  ): Promise<Models.ContainerGetAccessPolicyResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const containerAcl = await this.metadataStore.getContainerACL(
      accountName,
      containerName,
      options.leaseAccessConditions,
      context
    );

    const response: any = [];
    const responseArray = response as Models.SignedIdentifier[];
    const responseObject = response as Models.ContainerGetAccessPolicyHeaders & {
      statusCode: 200;
    };
    if (containerAcl.containerAcl !== undefined) {
      responseArray.push(...containerAcl.containerAcl);
    }
    responseObject.date = containerAcl.properties.lastModified;
    responseObject.blobPublicAccess = containerAcl.properties.publicAccess;
    responseObject.eTag = containerAcl.properties.etag;
    responseObject.lastModified = containerAcl.properties.lastModified;
    responseObject.requestId = context.contextID;
    responseObject.version = BLOB_API_VERSION;
    responseObject.statusCode = 200;

    // TODO: Need fix generator code since the output containerAcl can't be serialized correctly
    // tslint:disable-next-line:max-line-length
    // Correct responds： <?xml version="1.0" encoding="utf-8"?><SignedIdentifiers><SignedIdentifier><Id>123</Id><AccessPolicy><Start>2019-04-30T16:00:00.0000000Z</Start><Expiry>2019-12-31T16:00:00.0000000Z</Expiry><Permission>r</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>
    // tslint:disable-next-line:max-line-length
    // Current responds: <?xml version="1.0" encoding="UTF-8" standalone="yes"?><parsedResponse><Id>123</Id><AccessPolicy><Start>2019-04-30T16:00:00Z</Start><Expiry>2019-12-31T16:00:00Z</Expiry><Permission>r</Permission></AccessPolicy></parsedResponse>"
    return response;
  }

  /**
   * set container access policy
   *
   * @param {Models.ContainerSetAccessPolicyOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerSetAccessPolicyResponse>}
   * @memberof ContainerHandler
   */
  public async setAccessPolicy(
    options: Models.ContainerSetAccessPolicyOptionalParams,
    context: Context
  ): Promise<Models.ContainerSetAccessPolicyResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const date = blobCtx.startTime!;
    const eTag = newEtag();
    await this.metadataStore.setContainerACL(
      accountName,
      containerName,
      {
        lastModified: date,
        etag: eTag,
        publicAccess: options.access,
        containerAcl: options.containerAcl,
        leaseAccessConditions: options.leaseAccessConditions
      },
      context
    );

    const response: Models.ContainerSetAccessPolicyResponse = {
      date,
      eTag,
      lastModified: date,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 200
    };

    return response;
  }

  /**
   * acquire container lease
   *
   * @param {Models.ContainerAcquireLeaseOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerAcquireLeaseResponse>}
   * @memberof ContainerHandler
   */
  public async acquireLease(
    options: Models.ContainerAcquireLeaseOptionalParams,
    context: Context
  ): Promise<Models.ContainerAcquireLeaseResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;
    const res = await this.metadataStore.acquireContainerLease(
      accountName,
      containerName,
      options,
      context
    );

    const response: Models.ContainerAcquireLeaseResponse = {
      date: blobCtx.startTime!,
      eTag: res.properties.etag,
      lastModified: res.properties.lastModified,
      leaseId: res.leaseId,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 201
    };

    return response;
  }

  /**
   * release container lease
   *
   * @param {string} leaseId
   * @param {Models.ContainerReleaseLeaseOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerReleaseLeaseResponse>}
   * @memberof ContainerHandler
   */
  public async releaseLease(
    leaseId: string,
    options: Models.ContainerReleaseLeaseOptionalParams,
    context: Context
  ): Promise<Models.ContainerReleaseLeaseResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const res = await this.metadataStore.releaseContainerLease(
      accountName,
      containerName,
      leaseId,
      context
    );

    const response: Models.ContainerReleaseLeaseResponse = {
      date: blobCtx.startTime!,
      eTag: res.etag,
      lastModified: res.lastModified,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 200
    };

    return response;
  }

  /**
   * renew container lease
   *
   * @param {string} leaseId
   * @param {Models.ContainerRenewLeaseOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerRenewLeaseResponse>}
   * @memberof ContainerHandler
   */
  public async renewLease(
    leaseId: string,
    options: Models.ContainerRenewLeaseOptionalParams,
    context: Context
  ): Promise<Models.ContainerRenewLeaseResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const res = await this.metadataStore.renewContainerLease(
      accountName,
      containerName,
      leaseId,
      context
    );

    const response: Models.ContainerRenewLeaseResponse = {
      date: blobCtx.startTime!,
      eTag: res.properties.etag,
      lastModified: res.properties.lastModified,
      leaseId: res.leaseId,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 200
    };

    return response;
  }

  /**
   * break container lease
   *
   * @param {Models.ContainerBreakLeaseOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerBreakLeaseResponse>}
   * @memberof ContainerHandler
   */
  public async breakLease(
    options: Models.ContainerBreakLeaseOptionalParams,
    context: Context
  ): Promise<Models.ContainerBreakLeaseResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const res = await this.metadataStore.breakContainerLease(
      accountName,
      containerName,
      options.breakPeriod,
      context
    );

    const response: Models.ContainerBreakLeaseResponse = {
      date: blobCtx.startTime!,
      eTag: res.properties.etag,
      lastModified: res.properties.lastModified,
      leaseTime: res.leaseTime,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 202
    };

    return response;
  }

  /**
   * change container lease
   *
   * @param {string} leaseId
   * @param {string} proposedLeaseId
   * @param {Models.ContainerChangeLeaseOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerChangeLeaseResponse>}
   * @memberof ContainerHandler
   */
  public async changeLease(
    leaseId: string,
    proposedLeaseId: string,
    options: Models.ContainerChangeLeaseOptionalParams,
    context: Context
  ): Promise<Models.ContainerChangeLeaseResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;

    const res = await this.metadataStore.changeContainerLease(
      accountName,
      containerName,
      leaseId,
      proposedLeaseId,
      context
    );

    const response: Models.ContainerChangeLeaseResponse = {
      date: blobCtx.startTime!,
      eTag: res.properties.etag,
      lastModified: res.properties.lastModified,
      leaseId: res.leaseId,
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      statusCode: 200
    };

    return response;
  }

  /**
   * list blobs flat segments
   *
   * @param {Models.ContainerListBlobFlatSegmentOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerListBlobFlatSegmentResponse>}
   * @memberof ContainerHandler
   */
  public async listBlobFlatSegment(
    options: Models.ContainerListBlobFlatSegmentOptionalParams,
    context: Context
  ): Promise<Models.ContainerListBlobFlatSegmentResponse> {
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;
    await this.metadataStore.checkContainerExist(
      accountName,
      containerName,
      context
    );

    const request = context.request!;
    const marker = options.marker;
    const delimiter = "";
    options.marker = options.marker || "";
    let includeSnapshots: boolean = false;
    if (options.include !== undefined) {
      if (options.include.includes(Models.ListBlobsIncludeItem.Snapshots)) {
        includeSnapshots = true;
      }
    }
    const [blobs, nextMarker] = await this.metadataStore.listBlobs(
      accountName,
      containerName,
      undefined,
      options.prefix,
      options.maxresults,
      marker,
      includeSnapshots
    );

    const blobItems: Models.BlobItem[] = [];

    for (const blob of blobs) {
      blob.deleted = blob.deleted !== true ? undefined : true;
      blobItems.push(blob);
    }

    const serviceEndpoint = `${request.getEndpoint()}/${accountName}`;
    const response: Models.ContainerListBlobFlatSegmentResponse = {
      statusCode: 200,
      contentType: "application/xml",
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      date: context.startTime,
      serviceEndpoint,
      containerName,
      prefix: options.prefix || "",
      marker: options.marker,
      maxResults: options.maxresults || this.LIST_BLOBS_MAX_RESULTS_DEFAULT,
      delimiter,
      segment: {
        blobItems
      },
      nextMarker: `${nextMarker || ""}`
    };

    return response;
  }

  /**
   * List blobs hierarchy.
   *
   * @param {string} delimiter
   * @param {Models.ContainerListBlobHierarchySegmentOptionalParams} options
   * @param {Context} context
   * @returns {Promise<Models.ContainerListBlobHierarchySegmentResponse>}
   * @memberof ContainerHandler
   */
  public async listBlobHierarchySegment(
    delimiter: string,
    options: Models.ContainerListBlobHierarchySegmentOptionalParams,
    context: Context
  ): Promise<Models.ContainerListBlobHierarchySegmentResponse> {
    // TODO: Need update list out blobs lease properties with BlobHandler.updateLeaseAttributes()
    const blobCtx = new BlobStorageContext(context);
    const accountName = blobCtx.account!;
    const containerName = blobCtx.container!;
    await this.metadataStore.checkContainerExist(
      accountName,
      containerName,
      context
    );

    const request = context.request!;
    const marker = options.marker;
    delimiter = delimiter === "" ? "/" : delimiter;
    options.prefix = options.prefix || "";
    options.marker = options.marker || "";
    let includeSnapshots: boolean = false;
    if (options.include !== undefined) {
      if (options.include.includes(Models.ListBlobsIncludeItem.Snapshots)) {
        includeSnapshots = true;
      }
    }
    const [blobs, nextMarker] = await this.metadataStore.listBlobs(
      accountName,
      containerName,
      undefined,
      options.prefix,
      options.maxresults,
      marker,
      includeSnapshots
    );

    const blobItems: Models.BlobItem[] = [];
    const blobPrefixes: Models.BlobPrefix[] = [];
    const blobPrefixesSet = new Set<string>();

    const prefixLength = options.prefix.length;
    for (const blob of blobs) {
      const delimiterPosAfterPrefix = blob.name.indexOf(
        delimiter,
        prefixLength
      );

      // This is a blob
      if (delimiterPosAfterPrefix < 0) {
        blob.deleted = blob.deleted !== true ? undefined : true;
        blobItems.push(blob);
      } else {
        // This is a prefix
        const prefix = blob.name.substr(0, delimiterPosAfterPrefix + 1);
        blobPrefixesSet.add(prefix);
      }
    }

    const iter = blobPrefixesSet.values();
    let val;
    while (!(val = iter.next()).done) {
      blobPrefixes.push({ name: val.value });
    }

    const serviceEndpoint = `${request.getEndpoint()}/${accountName}`;
    const response: Models.ContainerListBlobHierarchySegmentResponse = {
      statusCode: 200,
      contentType: "application/xml",
      requestId: context.contextID,
      version: BLOB_API_VERSION,
      date: context.startTime,
      serviceEndpoint,
      containerName,
      prefix: options.prefix,
      marker: options.marker,
      maxResults: options.maxresults || this.LIST_BLOBS_MAX_RESULTS_DEFAULT,
      delimiter,
      segment: {
        blobItems,
        blobPrefixes
      },
      nextMarker: `${nextMarker || ""}`
    };

    return response;
  }

  /**
   * get account info
   *
   * @param {Context} context
   * @returns {Promise<Models.ContainerGetAccountInfoResponse>}
   * @memberof ContainerHandler
   */
  public async getAccountInfo(
    context: Context
  ): Promise<Models.ContainerGetAccountInfoResponse> {
    return getContainerGetAccountInfoResponse(context);
  }

  /**
   * get account info with headers
   *
   * @param {Context} context
   * @returns {Promise<Models.ContainerGetAccountInfoResponse>}
   * @memberof ContainerHandler
   */
  public async getAccountInfoWithHead(
    context: Context
  ): Promise<Models.ContainerGetAccountInfoResponse> {
    return getContainerGetAccountInfoResponse(context);
  }
}