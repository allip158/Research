/* 
 * Patient - simple class describing a patient to define Dataset schema
 */

/*
 * TODO: add more MAF columns for future features
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Patient implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String id;
	private List<String> mutations;
	private List<String> neoantigens;
	private List<String> neoepitopes;
	private List<String> wtantigens;
	private List<String> wtepitopes;
	private List<String> clusters;
	private Double label;
	private List<String> targets;
	
	public Patient() {
		mutations = new ArrayList<String>();
		neoantigens = new ArrayList<String>();
		neoepitopes = new ArrayList<String>();
		wtantigens = new ArrayList<String>();
		wtepitopes = new ArrayList<String>();
		clusters = new ArrayList<String>();
	}
	
	/* Copy Function */
	public Patient copy() {
		Patient duplicate = new Patient();
		duplicate.setMutations(mutations);
		duplicate.setClusters(clusters);
		duplicate.setId(id);
		duplicate.setNeoantigens(neoantigens);
		duplicate.setNeoepitopes(neoepitopes);
		duplicate.setTargets(targets);
		duplicate.setWtantigens(wtantigens);
		duplicate.setWtepitopes(wtepitopes);
		duplicate.setLabel(label);
		return duplicate;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public List<String> getMutations() {
		return mutations;
	}
	
	public void setMutations(List<String> mutations) {
		this.mutations = mutations;
	}	
	
	public void addMutation(String mutation) {
		this.mutations.add(mutation);
	}
	
	public List<String> getNeoepitopes() {
		return neoepitopes;
	}
	
	public void setNeoepitopes(List<String> neoepitopes) {
		this.neoepitopes = neoepitopes;
	}	
	
	public void addNeoepitope(String neoepitope) {
		this.neoepitopes.add(neoepitope);
	}
	
	public void addUniqueNeoepitope(String neoepitope) {
		if (!this.neoepitopes.contains(neoepitope)) {
			this.neoepitopes.add(neoepitope);
		}
	}
	
	public List<String> getWtantigens() {
		return this.wtantigens;
	}
	
	public void setWtantigens(List<String> wtantigens) {
		this.wtantigens = wtantigens;
	}
	
	public void addWtantigen(String wtantigen) {
		this.wtantigens.add(wtantigen);
	}
	
	public List<String> getWtepitopes() {
		return this.wtepitopes;
	}
	
	public void setWtepitopes(List<String> wtepitopes) {
		this.wtepitopes = wtepitopes;
	}
	
	public void addWtepitope(String wtepitope) {
		this.wtepitopes.add(wtepitope);
	}
		
	public List<String> getNeoantigens() {
		return neoantigens;
	}
	
	public void setNeoantigens(List<String> neoantigens) {
		this.neoantigens = neoantigens;
	}	
	
	public void addNeoantigen(String neoantigen) {
		this.neoantigens.add(neoantigen);
	}
	
	public void setClusters(List<String> clusters) {
		this.clusters = clusters;
	}
	
	public void addCluster(String cluster) {
		this.clusters.add(cluster);
	}
	
	public List<String> getClusters() {
		return this.clusters;
	}
	
	public Double getLabel() {
		return label;
	}
	
	public void setLabel(Double label) {
		this.label = label;
	}
	
	public List<String> getTargets() {
		return targets;
	}
	
	public void setTargets(List<String> targets) {
		this.targets = targets;
	}
	
}
