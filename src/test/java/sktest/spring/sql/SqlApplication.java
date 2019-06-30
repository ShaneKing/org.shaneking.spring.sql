package sktest.spring.sql;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
@SpringBootApplication
public class SqlApplication {

  public static void main(String[] args) {
    SpringApplication.run(SqlApplication.class, args);
  }

}
