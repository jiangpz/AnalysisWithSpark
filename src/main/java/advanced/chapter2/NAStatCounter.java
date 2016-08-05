package advanced.chapter2;

import java.io.Serializable;

import org.apache.spark.util.StatCounter;

public class NAStatCounter implements Serializable{
	private static final long serialVersionUID = 1L;
	StatCounter stats = new StatCounter();
	Long missing = 0L;
	
	public NAStatCounter(){
	}
	
	public NAStatCounter(Double x){
		this.add(x);
	}
	
	public NAStatCounter add(Double x) {
		if(Double.isNaN(x)) {
			missing += 1;
		} else {
			stats.merge(x);
		}
		return this;
	}
	
	public NAStatCounter merge(NAStatCounter other) {
		stats.merge(other.stats);
		missing += other.missing;
		return this;
	}

	@Override
	public String toString() {
		return "NAStatCounter [stats=" + stats + ", NaN=" + missing + "]";
	}
	
}
