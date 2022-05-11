/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package common.meta;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.16.0)", date = "2022-05-11")
public class ClientInfo implements org.apache.thrift.TBase<ClientInfo, ClientInfo._Fields>, java.io.Serializable, Cloneable, Comparable<ClientInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ClientInfo");

  private static final org.apache.thrift.protocol.TField IP_FIELD_DESC = new org.apache.thrift.protocol.TField("ip", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField RPC_PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("rpcPort", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField SOCKET_PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("socketPort", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField UID_FIELD_DESC = new org.apache.thrift.protocol.TField("uid", org.apache.thrift.protocol.TType.I32, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ClientInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ClientInfoTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String ip; // required
  public int rpcPort; // required
  public int socketPort; // required
  public int uid; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    IP((short)1, "ip"),
    RPC_PORT((short)2, "rpcPort"),
    SOCKET_PORT((short)3, "socketPort"),
    UID((short)4, "uid");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // IP
          return IP;
        case 2: // RPC_PORT
          return RPC_PORT;
        case 3: // SOCKET_PORT
          return SOCKET_PORT;
        case 4: // UID
          return UID;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __RPCPORT_ISSET_ID = 0;
  private static final int __SOCKETPORT_ISSET_ID = 1;
  private static final int __UID_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.IP, new org.apache.thrift.meta_data.FieldMetaData("ip", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.RPC_PORT, new org.apache.thrift.meta_data.FieldMetaData("rpcPort", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SOCKET_PORT, new org.apache.thrift.meta_data.FieldMetaData("socketPort", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.UID, new org.apache.thrift.meta_data.FieldMetaData("uid", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ClientInfo.class, metaDataMap);
  }

  public ClientInfo() {
  }

  public ClientInfo(
    java.lang.String ip,
    int rpcPort,
    int socketPort,
    int uid)
  {
    this();
    this.ip = ip;
    this.rpcPort = rpcPort;
    setRpcPortIsSet(true);
    this.socketPort = socketPort;
    setSocketPortIsSet(true);
    this.uid = uid;
    setUidIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ClientInfo(ClientInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetIp()) {
      this.ip = other.ip;
    }
    this.rpcPort = other.rpcPort;
    this.socketPort = other.socketPort;
    this.uid = other.uid;
  }

  public ClientInfo deepCopy() {
    return new ClientInfo(this);
  }

  @Override
  public void clear() {
    this.ip = null;
    setRpcPortIsSet(false);
    this.rpcPort = 0;
    setSocketPortIsSet(false);
    this.socketPort = 0;
    setUidIsSet(false);
    this.uid = 0;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getIp() {
    return this.ip;
  }

  public ClientInfo setIp(@org.apache.thrift.annotation.Nullable java.lang.String ip) {
    this.ip = ip;
    return this;
  }

  public void unsetIp() {
    this.ip = null;
  }

  /** Returns true if field ip is set (has been assigned a value) and false otherwise */
  public boolean isSetIp() {
    return this.ip != null;
  }

  public void setIpIsSet(boolean value) {
    if (!value) {
      this.ip = null;
    }
  }

  public int getRpcPort() {
    return this.rpcPort;
  }

  public ClientInfo setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
    setRpcPortIsSet(true);
    return this;
  }

  public void unsetRpcPort() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __RPCPORT_ISSET_ID);
  }

  /** Returns true if field rpcPort is set (has been assigned a value) and false otherwise */
  public boolean isSetRpcPort() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __RPCPORT_ISSET_ID);
  }

  public void setRpcPortIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __RPCPORT_ISSET_ID, value);
  }

  public int getSocketPort() {
    return this.socketPort;
  }

  public ClientInfo setSocketPort(int socketPort) {
    this.socketPort = socketPort;
    setSocketPortIsSet(true);
    return this;
  }

  public void unsetSocketPort() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SOCKETPORT_ISSET_ID);
  }

  /** Returns true if field socketPort is set (has been assigned a value) and false otherwise */
  public boolean isSetSocketPort() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SOCKETPORT_ISSET_ID);
  }

  public void setSocketPortIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SOCKETPORT_ISSET_ID, value);
  }

  public int getUid() {
    return this.uid;
  }

  public ClientInfo setUid(int uid) {
    this.uid = uid;
    setUidIsSet(true);
    return this;
  }

  public void unsetUid() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __UID_ISSET_ID);
  }

  /** Returns true if field uid is set (has been assigned a value) and false otherwise */
  public boolean isSetUid() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __UID_ISSET_ID);
  }

  public void setUidIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __UID_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case IP:
      if (value == null) {
        unsetIp();
      } else {
        setIp((java.lang.String)value);
      }
      break;

    case RPC_PORT:
      if (value == null) {
        unsetRpcPort();
      } else {
        setRpcPort((java.lang.Integer)value);
      }
      break;

    case SOCKET_PORT:
      if (value == null) {
        unsetSocketPort();
      } else {
        setSocketPort((java.lang.Integer)value);
      }
      break;

    case UID:
      if (value == null) {
        unsetUid();
      } else {
        setUid((java.lang.Integer)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case IP:
      return getIp();

    case RPC_PORT:
      return getRpcPort();

    case SOCKET_PORT:
      return getSocketPort();

    case UID:
      return getUid();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case IP:
      return isSetIp();
    case RPC_PORT:
      return isSetRpcPort();
    case SOCKET_PORT:
      return isSetSocketPort();
    case UID:
      return isSetUid();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof ClientInfo)
      return this.equals((ClientInfo)that);
    return false;
  }

  public boolean equals(ClientInfo that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_ip = true && this.isSetIp();
    boolean that_present_ip = true && that.isSetIp();
    if (this_present_ip || that_present_ip) {
      if (!(this_present_ip && that_present_ip))
        return false;
      if (!this.ip.equals(that.ip))
        return false;
    }

    boolean this_present_rpcPort = true;
    boolean that_present_rpcPort = true;
    if (this_present_rpcPort || that_present_rpcPort) {
      if (!(this_present_rpcPort && that_present_rpcPort))
        return false;
      if (this.rpcPort != that.rpcPort)
        return false;
    }

    boolean this_present_socketPort = true;
    boolean that_present_socketPort = true;
    if (this_present_socketPort || that_present_socketPort) {
      if (!(this_present_socketPort && that_present_socketPort))
        return false;
      if (this.socketPort != that.socketPort)
        return false;
    }

    boolean this_present_uid = true;
    boolean that_present_uid = true;
    if (this_present_uid || that_present_uid) {
      if (!(this_present_uid && that_present_uid))
        return false;
      if (this.uid != that.uid)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetIp()) ? 131071 : 524287);
    if (isSetIp())
      hashCode = hashCode * 8191 + ip.hashCode();

    hashCode = hashCode * 8191 + rpcPort;

    hashCode = hashCode * 8191 + socketPort;

    hashCode = hashCode * 8191 + uid;

    return hashCode;
  }

  @Override
  public int compareTo(ClientInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetIp(), other.isSetIp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ip, other.ip);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetRpcPort(), other.isSetRpcPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRpcPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rpcPort, other.rpcPort);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSocketPort(), other.isSetSocketPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSocketPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.socketPort, other.socketPort);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetUid(), other.isSetUid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.uid, other.uid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ClientInfo(");
    boolean first = true;

    sb.append("ip:");
    if (this.ip == null) {
      sb.append("null");
    } else {
      sb.append(this.ip);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("rpcPort:");
    sb.append(this.rpcPort);
    first = false;
    if (!first) sb.append(", ");
    sb.append("socketPort:");
    sb.append(this.socketPort);
    first = false;
    if (!first) sb.append(", ");
    sb.append("uid:");
    sb.append(this.uid);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ClientInfoStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ClientInfoStandardScheme getScheme() {
      return new ClientInfoStandardScheme();
    }
  }

  private static class ClientInfoStandardScheme extends org.apache.thrift.scheme.StandardScheme<ClientInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // IP
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.ip = iprot.readString();
              struct.setIpIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // RPC_PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.rpcPort = iprot.readI32();
              struct.setRpcPortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SOCKET_PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.socketPort = iprot.readI32();
              struct.setSocketPortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // UID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.uid = iprot.readI32();
              struct.setUidIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ClientInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.ip != null) {
        oprot.writeFieldBegin(IP_FIELD_DESC);
        oprot.writeString(struct.ip);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(RPC_PORT_FIELD_DESC);
      oprot.writeI32(struct.rpcPort);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SOCKET_PORT_FIELD_DESC);
      oprot.writeI32(struct.socketPort);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(UID_FIELD_DESC);
      oprot.writeI32(struct.uid);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ClientInfoTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ClientInfoTupleScheme getScheme() {
      return new ClientInfoTupleScheme();
    }
  }

  private static class ClientInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<ClientInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetIp()) {
        optionals.set(0);
      }
      if (struct.isSetRpcPort()) {
        optionals.set(1);
      }
      if (struct.isSetSocketPort()) {
        optionals.set(2);
      }
      if (struct.isSetUid()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetIp()) {
        oprot.writeString(struct.ip);
      }
      if (struct.isSetRpcPort()) {
        oprot.writeI32(struct.rpcPort);
      }
      if (struct.isSetSocketPort()) {
        oprot.writeI32(struct.socketPort);
      }
      if (struct.isSetUid()) {
        oprot.writeI32(struct.uid);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.ip = iprot.readString();
        struct.setIpIsSet(true);
      }
      if (incoming.get(1)) {
        struct.rpcPort = iprot.readI32();
        struct.setRpcPortIsSet(true);
      }
      if (incoming.get(2)) {
        struct.socketPort = iprot.readI32();
        struct.setSocketPortIsSet(true);
      }
      if (incoming.get(3)) {
        struct.uid = iprot.readI32();
        struct.setUidIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

