package com

package object example {

  case class AuthenticationInfo(principal: String, metadata: Option[Metadata])

  case class AuthorizationInfo(
      granted: Boolean,
      operation: String,
      resourceType: String,
      resourceName: String,
      patternType: String,
      superUserAuthorization: Option[Boolean],
      rbacAuthorization: Option[RbacAuthorization]
  )

  case class RbacAuthorization(
      role: String,
      scope: Scope
  )

  case class Scope(outerScope: Array[String])

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

  case class Request(correlation_id: String, client_id: Option[String])

  case class RequestMetadata(client_address: Option[String])

  case class Result(status: String, message: Option[String])

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
      serviceName: Option[String],
      methodName: Option[String],
      resourceName: Option[String]
     // confluentRouting: ConfluentRouting
  )

}
