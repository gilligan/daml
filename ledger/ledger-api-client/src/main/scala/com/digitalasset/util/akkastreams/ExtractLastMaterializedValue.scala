// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util.akkastreams

import akka.stream.scaladsl.Flow
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.concurrent.{Future, Promise}

/**
  * Takes the input data, applies the provided transformation function, and completes its materialized value with it.
  */
class ExtractLastMaterializedValue[T, Mat](toMaterialized: T => Option[Mat])
    extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Option[Mat]]] {

  val inlet: Inlet[T] = Inlet[T]("in")
  val outlet: Outlet[T] = Outlet[T]("out")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, Future[Option[Mat]]) = {
    val promise = Promise[Option[Mat]]()

    val logic = new GraphStageLogic(shape) {

      var lastSeenValue: Option[Mat] = None

      setHandler(
        inlet,
        new InHandler {
          override def onPush(): Unit = {
            val input = grab(inlet)
            push(outlet, input)
            val materialized = toMaterialized(input)
            if (materialized.isDefined)
              lastSeenValue = materialized
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.tryFailure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.trySuccess(lastSeenValue)
            super.onUpstreamFinish()
          }
        }
      )

      setHandler(
        outlet,
        new OutHandler {
          override def onPull(): Unit = pull(inlet)

          override def onDownstreamFinish(): Unit = {
            promise.trySuccess(lastSeenValue)
            super.onDownstreamFinish()
          }
        }
      )

    }

    logic -> promise.future
  }

  override def shape: FlowShape[T, T] = FlowShape(inlet, outlet)
}

object ExtractLastMaterializedValue {
  def apply[T, Mat](toOutputOrMaterialized: T => Option[Mat]): Flow[T, T, Future[Option[Mat]]] =
    Flow.fromGraph(new ExtractLastMaterializedValue[T, Mat](toOutputOrMaterialized))
}
