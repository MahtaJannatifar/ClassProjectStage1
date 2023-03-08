import javax.swing.text.html.Option;
import java.util.Optional;
import java.util.stream.Stream;

public enum AttributeType {
  INT,
  VARCHAR,
  DOUBLE;

  public static AttributeType findByValue(String value) {
    Optional<AttributeType> type = Stream
            .of(AttributeType.values())
            .filter(v -> v.toString().equals(value))
            .findFirst();

    if (type.isPresent()) {
      return type.get();
    }

    throw new RuntimeException(
            String.format(
                    "It was not possible to find an attribute type for value: %s",
                    value
            )
    );
  }

}
