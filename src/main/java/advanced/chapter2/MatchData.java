package advanced.chapter2;

import java.io.Serializable;
import java.util.Arrays;

public class MatchData implements Serializable{
	private static final long serialVersionUID = 1L;
	private Integer id1;
	private Integer id2;
	private Double[] scores; 
	private Boolean matched;
	public Integer getId1() {
		return id1;
	}
	public void setId1(Integer id1) {
		this.id1 = id1;
	}
	public Integer getId2() {
		return id2;
	}
	public void setId2(Integer id2) {
		this.id2 = id2;
	}
	public Double[] getScores() {
		return scores;
	}
	public void setScores(Double[] scores) {
		this.scores = scores;
	}
	public Boolean getMatched() {
		return matched;
	}
	public void setMatched(Boolean matched) {
		this.matched = matched;
	}
	
	@Override
	public String toString() {
		return "MatchData [id1=" + id1 + ", id2=" + id2 + ", scores=" + Arrays.toString(scores) + ", matched=" + matched
				+ "]";
	}
}
