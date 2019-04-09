# java集合框架笔记
## 概述
最初java提供了Vector、Stack、Hashtable、BitSet与Enumeration接口  
在Java SE 1.2后就发布了很多集合的框架，下面从顶层接口出发来说明集合框架。  

1. Iterable接口  
  public interface Iterable<T> {
      Iterator<T> iterator();
  }
