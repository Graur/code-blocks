import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;

@Repository
public interface PodErrorRepository extends JpaRepository<PodError, Long> {
    
    @Query("SELECT AVG(pe.errorsLastHour) FROM PodError pe")
    Double calculateAverageErrorsLastHour();
    
    @Query("SELECT AVG(pe.errorsLast3Days) FROM PodError pe")
    Double calculateAverageErrorsLast3Days();
    
    void deleteByTimestampBefore(LocalDateTime timestamp);
}

@Repository
public interface AggregatedErrorRepository extends JpaRepository<AggregatedError, Long> {
}

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "pod_errors")
public class PodError {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String podId;
    private LocalDateTime timestamp;
    private double errorsLastHour;
    private double errorsLast3Days;

    public PodError() {}

    public PodError(String podId, LocalDateTime timestamp, double errorsLastHour, double errorsLast3Days) {
        this.podId = podId;
        this.timestamp = timestamp;
        this.errorsLastHour = errorsLastHour;
        this.errorsLast3Days = errorsLast3Days;
    }

    // Getters and Setters
}

@Entity
@Table(name = "aggregated_errors")
public class AggregatedError {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private LocalDateTime timestamp;
    private Double avgErrorsLastHour;
    private Double avgErrorsLast3Days;

    public AggregatedError() {}

    public AggregatedError(LocalDateTime timestamp, Double avgErrorsLastHour, Double avgErrorsLast3Days) {
        this.timestamp = timestamp;
        this.avgErrorsLastHour = avgErrorsLastHour;
        this.avgErrorsLast3Days = avgErrorsLast3Days;
    }

    // Getters and Setters
}

//2

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class ErrorStatisticsService {
    
    private final PrometheusClient prometheusClient;
    private final PodErrorRepository podErrorRepository;
    private final AggregatedErrorRepository aggregatedErrorRepository;
    private final TaskScheduler taskScheduler;
    
    @Value("${pod.id}")
    private String podId;

    public ErrorStatisticsService(PrometheusClient prometheusClient, PodErrorRepository podErrorRepository, AggregatedErrorRepository aggregatedErrorRepository, TaskScheduler taskScheduler) {
        this.prometheusClient = prometheusClient;
        this.podErrorRepository = podErrorRepository;
        this.aggregatedErrorRepository = aggregatedErrorRepository;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void scheduleTasks() {
        taskScheduler.schedule(this::collectAndStoreStatistics, new CronTrigger("0 0 * * * *"));
        taskScheduler.schedule(this::aggregateStatistics, new CronTrigger("0 10 * * * *"));
        taskScheduler.schedule(this::cleanOldEntries, new CronTrigger("0 0 0 * * *"));
    }

    @Transactional
    public void collectAndStoreStatistics() {
        double errorsLastHour = prometheusClient.getErrorsLastHour();
        double errorsLast3Days = prometheusClient.getErrorsLast3Days();
        
        PodError podError = new PodError(podId, LocalDateTime.now(), errorsLastHour, errorsLast3Days);
        podErrorRepository.save(podError);
    }

    @SchedulerLock(name = "aggregate_statistics", lockAtMostFor = "10m", lockAtLeastFor = "1m")
    @Transactional
    public void aggregateStatistics() {
        Double avgErrorsLastHour = podErrorRepository.calculateAverageErrorsLastHour();
        Double avgErrorsLast3Days = podErrorRepository.calculateAverageErrorsLast3Days();
        
        AggregatedError aggregatedError = new AggregatedError(LocalDateTime.now(), avgErrorsLastHour, avgErrorsLast3Days);
        aggregatedErrorRepository.save(aggregatedError);
    }
    
    @Transactional
    public void cleanOldEntries() {
        podErrorRepository.deleteOldEntries(LocalDateTime.now().minusHours(24));
    }
    
    @Bean(name = "customTaskScheduler")
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("ErrorStatsScheduler-");
        scheduler.initialize();
        return scheduler;
    }
}

//3
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class ErrorStatisticsService {
    
    private final PrometheusClient prometheusClient;
    private final JdbcTemplate jdbcTemplate;
    
    @Value("${pod.id}")
    private String podId;

    public ErrorStatisticsService(PrometheusClient prometheusClient, JdbcTemplate jdbcTemplate) {
        this.prometheusClient = prometheusClient;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Scheduled(fixedRate = 3600000) // Раз в час
    @Transactional
    public void collectAndStoreStatistics() {
        double errorsLastHour = prometheusClient.getErrorsLastHour();
        double errorsLast3Days = prometheusClient.getErrorsLast3Days();
        
        jdbcTemplate.update("""
            INSERT INTO pod_errors (pod_id, timestamp, errors_last_hour, errors_last_3_days) 
            VALUES (?, ?, ?, ?) 
            ON CONFLICT (pod_id, timestamp) DO UPDATE 
            SET errors_last_hour = EXCLUDED.errors_last_hour, 
                errors_last_3_days = EXCLUDED.errors_last_3_days
        """, podId, LocalDateTime.now(), errorsLastHour, errorsLast3Days);
    }

    @Scheduled(fixedRate = 3600000)
    @SchedulerLock(name = "aggregate_statistics", lockAtMostFor = "10m", lockAtLeastFor = "1m")
    @Transactional
    public void aggregateStatistics() {
        jdbcTemplate.update("""
            INSERT INTO aggregated_errors (timestamp, avg_errors_last_hour, avg_errors_last_3_days) 
            SELECT NOW(), AVG(errors_last_hour), AVG(errors_last_3_days) FROM pod_errors
            ON CONFLICT (timestamp) DO UPDATE 
            SET avg_errors_last_hour = EXCLUDED.avg_errors_last_hour, 
                avg_errors_last_3_days = EXCLUDED.avg_errors_last_3_days
        """ );
    }
    
    @Scheduled(cron = "0 0 0 * * ?") // Каждый день в полночь
    @Transactional
    public void cleanOldEntries() {
        jdbcTemplate.update("DELETE FROM pod_errors WHERE timestamp < NOW() - INTERVAL '24 hours'");
    }
}

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;

@Component
public class PrometheusClient {
    
    private final RestTemplate restTemplate;
    
    @Value("${prometheus.url}")
    private String prometheusUrl;
    
    public PrometheusClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
    
    public double getErrorsLastHour() {
        String query = "rate(log_errors[1h])";
        return executePrometheusQuery(query);
    }
    
    public double getErrorsLast3Days() {
        String query = "rate(log_errors[3d])";
        return executePrometheusQuery(query);
    }
    
    private double executePrometheusQuery(String query) {
        String url = String.format("%s/api/v1/query?query=%s", prometheusUrl, query);
        PrometheusResponse response = restTemplate.getForObject(url, PrometheusResponse.class);
        return response != null && response.getData() != null ? response.getData().getValue() : 0.0;
    }
}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PrometheusResponse {
    @JsonProperty("data")
    private PrometheusData data;

    public PrometheusData getData() {
        return data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PrometheusData {
        @JsonProperty("result")
        private List<PrometheusResult> result;

        public double getValue() {
            if (result != null && !result.isEmpty()) {
                return Double.parseDouble(result.get(0).getValue().get(1));
            }
            return 0.0;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PrometheusResult {
        @JsonProperty("value")
        private List<String> value;

        public List<String> getValue() {
            return value;
        }
    }
}
