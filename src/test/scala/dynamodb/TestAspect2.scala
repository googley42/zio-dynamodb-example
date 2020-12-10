package dynamodb

import zio.test._
import zio.{ZIO, ZManaged}

object TestAspect2 {

  /**
    * Constructs an aspect that evaluates all tests between two effects,
    * `before` and `after`, where the result of `before` can be used in
    * `after`.
    */
  def aroundAll[R0, E0, A0](
    before: ZIO[R0, E0, A0]
  )(after: A0 => ZIO[R0, Nothing, Any]): TestAspect[Nothing, R0, E0, Any] =
    new TestAspect[Nothing, R0, E0, Any] {

      def some[R <: R0, E >: E0](predicate: String => Boolean, spec: ZSpec[R, E]): ZSpec[R, E] = {
        def aroundAll[R <: R0, E >: E0, A](
          specs: ZManaged[R, TestFailure[E], Vector[Spec[R, TestFailure[E], TestSuccess]]]
        ): ZManaged[R, TestFailure[E], Vector[Spec[R, TestFailure[E], TestSuccess]]] =
          ZManaged.make(before)(after).mapError(TestFailure.fail) *> specs
        def around[R <: R0, E >: E0, A](
          test: ZIO[R, TestFailure[E], TestSuccess]
        ): ZIO[R, TestFailure[E], TestSuccess] =
          before.mapError(TestFailure.fail).bracket(after)(_ => test)
        spec.caseValue match {
          case Spec.SuiteCase(label, specs, exec)      => Spec.suite(label, aroundAll(specs), exec)
          case Spec.TestCase(label, test, annotations) => Spec.test(label, around(test), annotations)
        }
      }
    }
}
