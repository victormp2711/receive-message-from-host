package com.gracaconsultores.messages.models;

import lombok.Data;

import java.util.UUID;

@Data
public class BigSerializedObject {
  private String id;
  private String title;
  private String url;

  public BigSerializedObject(String string, String cooper, String pruebasDeWebclient) {
  }

  @Override
  public String toString() {
    return ": {" +
      "\"" + "id" + "\"" + ": " + id  +
      ",\"" + "title" + "\"" + ": \"" + title + "\"" +
      ",\"" + "url" + "\"" + ": \"" + url + "\"" +
      "}";
  }

  public BigSerializedObject() {
    StringBuilder sb = new StringBuilder(UUID.randomUUID().toString());

    for (int i = 0; i < 20; i++) {
      sb.append(sb);
    }
    this.id = sb.toString();
  }
}

