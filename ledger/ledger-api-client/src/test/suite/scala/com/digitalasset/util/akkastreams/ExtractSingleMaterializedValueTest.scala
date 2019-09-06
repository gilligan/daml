// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util.akkastreams

import akka.stream.scaladsl.{Keep, Sink, Source}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ExtractSingleMaterializedValueTest
    extends WordSpec
    with Matchers
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  private val discriminator = { i: Int =>
    if (i < 0) Some(i) else None
  }

  private val elemsThatPassThrough = 0.to(10).toVector

  ExtractLastMaterializedValue.getClass.getSimpleName when {

    "there's a single valid value" should {
      "extract it" in {
        val elemToExtract = -1

        val elements = elemToExtract +: elemsThatPassThrough
        val (extractedF, restF) = processElements(Random.shuffle(elements))

        whenReady(extractedF)(_ shouldEqual Some(elemToExtract))
        whenReady(restF)(_ should contain theSameElementsAs elements)
      }
    }

    "there are multiple valid values" should {
      "extract the last" in {
        val elemToExtract = -1
        val otherCandidateShuffledIn = -2

        val elements = Random.shuffle(otherCandidateShuffledIn +: elemsThatPassThrough) :+ elemToExtract
        val (extractedF, restF) = processElements(elements)

        whenReady(extractedF)(_ shouldEqual Some(elemToExtract))
        whenReady(restF)(_ should contain theSameElementsAs elements)
      }
    }

    "there are no valid values" should {
      "complete the materialized future with None" in {

        val (extractedF, restF) =
          processElements(Random.shuffle(elemsThatPassThrough))

        whenReady(extractedF)(_ shouldEqual None)
        whenReady(restF)(_.sorted shouldEqual elemsThatPassThrough)
      }
    }

  }

  private def processElements(elements: Iterable[Int]) = {
    Source
      .fromIterator(() => elements.iterator)
      .viaMat(ExtractLastMaterializedValue(discriminator))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()
  }
}
