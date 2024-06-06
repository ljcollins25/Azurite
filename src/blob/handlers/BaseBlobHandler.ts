import { URLBuilder } from "@azure/ms-rest-js";
import axios, { AxiosResponse, AxiosRequestHeaders } from "axios";
import { URL } from "url";

import BlobStorageContext from "../context/BlobStorageContext";
import StorageErrorFactory from "../errors/StorageErrorFactory";
import Context from "../generated/Context";
import BaseHandler from "./BaseHandler";
import { HeaderConstants } from "../utils/constants";
import { parseXML } from "../generated/utils/xml";
import { Readable } from "stream";
import Operation from "../generated/artifacts/operation";

/**
 * BaseBlobHandler
 *
 * @export
 * @class BaseBlobHandler
 * @extends {BaseHandler}
 */
export default class BaseBlobHandler
  extends BaseHandler {


  protected NewUriFromCopySource(copySource: string, context: Context): URL {
    try {
      return new URL(copySource)
    }
    catch {
      throw StorageErrorFactory.getInvalidHeaderValue(
        context.contextId,
        {
          HeaderName: "x-ms-copy-source",
          HeaderValue: copySource
        })
    }
  }

  protected async getCopySourceContent(
    copySource: string,
    sourceAccount: string,
    context: Context)
    : Promise<Readable> {
    const sourceHeaders: AxiosRequestHeaders = {};

    this.copyHeader(context, sourceHeaders, "x-ms-source-range", HeaderConstants.RANGE);

    const operation = Operation[context.context.operation];
    const response: AxiosResponse = await axios.get(copySource,
      {
        headers: sourceHeaders,
        responseType: "stream",
        // Instructs axios to not throw an error for non-2xx responses
        validateStatus: () => true
      }
    );

    if (response.status >= 200 && response.status < 300) {
      this.logger.debug(
        `${operation}() Successfully validated access to source account ${sourceAccount}`,
        context.contextId
      );

      return response.data;
    } else {
      this.logger.debug(
        `${operation}() Access denied to source account ${sourceAccount} StatusCode=${response.status}, AuthenticationErrorDetail=${response.data}`,
        context.contextId
      );

      if (response.status === 404) {
        throw StorageErrorFactory.getCannotVerifyCopySource(
          context.contextId!,
          response.status,
          "The specified resource does not exist"
        );
      } else {
        // For non-successful responses attempt to unwrap the error message from the metadata call.
        let message: string =
          "Could not verify the copy source within the specified time.";
        if (
          response.headers[HeaderConstants.CONTENT_TYPE] ===
          "application/xml"
        ) {
          const authenticationError = await parseXML(response.data);
          if (authenticationError.Message !== undefined) {
            message = authenticationError.Message.replace(/\n+/gm, "");
          }
        }

        throw StorageErrorFactory.getCannotVerifyCopySource(
          context.contextId!,
          response.status,
          message
        );
      }
    }
  }

  private copyHeader(context: Context, targetHeaders: AxiosRequestHeaders, name: string, targetName: string) {
    const headerValue = context.request?.getHeader(name);
    if (headerValue !== undefined) {
      targetHeaders[targetName] = headerValue;
    }

  }

  protected async validateCopySource(copySource: string, sourceAccount: string, context: Context): Promise<void> {
    // Currently the only cross-account copy support is from/to the same Azurite instance. In either case access
    // is determined by performing a request to the copy source to see if the authentication is valid.
    const blobCtx = new BlobStorageContext(context);

    const currentServer = blobCtx.request!.getHeader("Host") || "";
    const url = this.NewUriFromCopySource(copySource, context);
    if (currentServer !== url.host) {
      this.logger.error(
        `BlobHandler:startCopyFromURL() Source account ${url} is not on the same Azurite instance as target account ${blobCtx.account}`,
        context.contextId
      );

      throw StorageErrorFactory.getCannotVerifyCopySource(
        context.contextId!,
        404,
        "The specified resource does not exist"
      );
    }

    this.logger.debug(
      `BlobHandler:startCopyFromURL() Validating access to the source account ${sourceAccount}`,
      context.contextId
    );

    // In order to retrieve proper error details we make a metadata request to the copy source. If we instead issue
    // a HEAD request then the error details are not returned and reporting authentication failures to the caller
    // becomes a black box.
    const metadataUrl = URLBuilder.parse(copySource);
    metadataUrl.setQueryParameter("comp", "metadata");
    const validationResponse: AxiosResponse = await axios.get(
      metadataUrl.toString(),
      {
        // Instructs axios to not throw an error for non-2xx responses
        validateStatus: () => true
      }
    );
    if (validationResponse.status === 200) {
      this.logger.debug(
        `BlobHandler:startCopyFromURL() Successfully validated access to source account ${sourceAccount}`,
        context.contextId
      );
    } else {
      this.logger.debug(
        `BlobHandler:startCopyFromURL() Access denied to source account ${sourceAccount} StatusCode=${validationResponse.status}, AuthenticationErrorDetail=${validationResponse.data}`,
        context.contextId
      );

      if (validationResponse.status === 404) {
        throw StorageErrorFactory.getCannotVerifyCopySource(
          context.contextId!,
          validationResponse.status,
          "The specified resource does not exist"
        );
      } else {
        // For non-successful responses attempt to unwrap the error message from the metadata call.
        let message: string =
          "Could not verify the copy source within the specified time.";
        if (
          validationResponse.headers[HeaderConstants.CONTENT_TYPE] ===
          "application/xml"
        ) {
          const authenticationError = await parseXML(validationResponse.data);
          if (authenticationError.Message !== undefined) {
            message = authenticationError.Message.replace(/\n+/gm, "");
          }
        }

        throw StorageErrorFactory.getCannotVerifyCopySource(
          context.contextId!,
          validationResponse.status,
          message
        );
      }
    }
  }
}
