package com.oliver.monitoringSpark;

import java.io.Serializable;

public class ClaseTonta implements Serializable{

	private int num;

	public ClaseTonta(int num) {
		this.num = num;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

}
