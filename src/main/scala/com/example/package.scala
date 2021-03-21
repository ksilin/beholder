package com

package object example {

  case class AuthenticationInfo(principal: String, metadata: Option[Metadata])

  case class AuthorizationInfo(
      granted: Boolean,
      operation: String,
      resourceType: String,
      resourceName: String,
      patternType: String,
      superUserAuthorization: Boolean
  )

  case class Data(
      serviceName: String,
      methodName: String,
      resourceName: String,
      authenticationInfo: AuthenticationInfo,
      authorizationInfo: Option[AuthorizationInfo],
      request: Option[Request],
      requestMetadata: RequestMetadata,
      result: Option[Result]
  )

  case class Metadata(mechanism: String, identifier: String)

  case class Request(correlation_id: String, client_id: String)

  case class RequestMetadata(client_address: String)

  case class Result(status: String, message: String)

  case class ConfluentRouting(route: String)

  case class AuditLogEntry(
      data: Data,
      id: String,
      source: String,
      specversion: String,
      `type`: String,
      datacontenttype: String,
      subject: String,
      time: String,
      confluentRouting: ConfluentRouting
  )

}
