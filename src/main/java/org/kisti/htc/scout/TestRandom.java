package org.kisti.htc.scout;

public class TestRandom {

	public static void main(String[] args) {
		double random = Math.random();
		long randomRound = Math.round(random * 10);
		for (int i = 0; i < 100; i++) {
			System.out.println(Math.round(Math.random() * 10));
		}
		System.out.println(randomRound);
	}
}
