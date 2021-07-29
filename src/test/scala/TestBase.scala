import com.example.{ConfigBase, FutureConverter}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

class TestBase extends AnyFreeSpec with ConfigBase
  with LogSupport
  with Matchers
  with BeforeAndAfterAll
  with FutureConverter {
}
