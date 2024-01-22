#pragma once

#include <Arduino.h>
#include <CborEncoder.h>

/**
 * Use string value if non-null, otherwise use numeric value.
 */
template <typename Enum>
class Shortcut {
  public:
    enum class Type {
      ABSENT,
      FLASH_STRING,
      RAM_STRING,
      NUM,
    };
    Shortcut() : type(Type::ABSENT) { }
    Shortcut(const __FlashStringHelper *str) : type(Type::FLASH_STRING), flash_str(str) { }
    Shortcut(const char *str) : type(Type::RAM_STRING), ram_str(str) { }
    Shortcut(Enum num) : type(Type::NUM), num(num) { }

    Type type;

    void to_cbor(CborWriter& w);

    union {
      const char *ram_str;
      const __FlashStringHelper *flash_str;
      Enum num;
    };
};

enum class ConfigKey : uint8_t {
  ChannelId = 1,
  Quantity = 2,
  Unit = 3,
  Sensor = 4,
  ItemType = 5,
};

enum class ItemType : uint8_t {
  Node = 1,
  Channel = 2,
};

enum class Quantity : uint8_t {
  Temperature = 1,
  Humidity = 2,
  Voltage = 3,
};

enum class Unit : uint8_t {
  DegreeCelsius = 1,
  PercentRelativeHumidity = 2,
  Volt = 3,
  // TODO: mV and other scaling? Separate unit, or encoding detail?
};

enum class Sensor : uint8_t {
  Si7021 = 1,
};

enum class DataKey : uint8_t {
  ChannelId = 1,
  Value = 2,
};

class Variable {
  public:
    Variable() { }

    Variable& setQuantity(Shortcut<Quantity> quantity) { this->quantity = quantity; return *this; }
    Variable& setUnit(Shortcut<Unit> unit) { this->unit = unit; return *this; }
    Variable& setSensor(Shortcut<Sensor> sensor) { this->sensor = sensor; return *this; }

    uint8_t index;
    Shortcut<Quantity> quantity;
    Shortcut<Unit> unit;
    Shortcut<Sensor> sensor;

    // TODO: Encoding?
    // TODO: Extra key-value pairs. How to allocate memory? Template
    // argument with count? Or external and store poiner?
    //
    Variable *next = nullptr;
};

template <size_t MaxVariables>
class Device {
  public:
    // TODO: Do we need to keep a (linked) list of variables?
    Variable& addVariable() {
      this->variables[this->num_variables].index = num_variables;
      return this->variables[this->num_variables++];
    }

    void renderConfig(CborOutput& out);

    uint8_t num_variables = 0;
    Variable variables[MaxVariables];
};

template <size_t MaxVariables>
class Packet {
  public:
    Packet(CborOutput& out);
    // TODO: Type of value?
    // TODO: Extra key-value pairs?
    // TODO: Store variables? Or generate packet directly?
    void addValue(const Variable& variable, int32_t value);
    CborOutput& out;
    CborWriter writer;
};

template <size_t MaxVariables>
void Device<MaxVariables>::renderConfig(CborOutput& out) {
  CborWriter w(out);
  w.writeArray(this->num_variables + 1);

  // TODO: This number must match the number of entries below! Should be
  // determined automatically.
  w.writeMap(2);
  w.writeInt((long)ConfigKey::ItemType);
  w.writeInt((long)ItemType::Node);

  w.writeString("experimental");
  w.writeInt(1);

  for (uint8_t i=0; i < this->num_variables; ++i) {
    // TODO: This number must match the number of entries below! Should be
    // determined automatically.
    w.writeMap(5);
    auto& variable = this->variables[i];

    w.writeInt((long)ConfigKey::ItemType);
    w.writeInt((long)ItemType::Channel);

    w.writeInt((long)ConfigKey::ChannelId);
    w.writeInt(i);

    w.writeInt((long)ConfigKey::Quantity);
    variable.quantity.to_cbor(w);

    w.writeInt((long)ConfigKey::Unit);
    variable.unit.to_cbor(w);

    w.writeInt((long)ConfigKey::Sensor);
    variable.sensor.to_cbor(w);
  }

  // TODO: Handle overflow somewhere
}

template <size_t MaxVariables>
Packet<MaxVariables>::Packet(CborOutput& out) : out(out), writer(out) {
  // TODO: This number must match the actual number of variables! Should be
  // determined automatically.
  this->writer.writeArray(MaxVariables);
}

template <size_t MaxVariables>
void Packet<MaxVariables>::addValue(const Variable& variable, int32_t value) {
  this->writer.writeMap(2);

  this->writer.writeInt((long)DataKey::ChannelId);
  this->writer.writeInt(variable.index);

  this->writer.writeInt((long)DataKey::Value);
  this->writer.writeInt(value);
  // TODO: Encoding of value
}

template <typename Enum>
void Shortcut<Enum>::to_cbor(CborWriter& w) {
  switch (this->type) {
    case Type::ABSENT:
      w.writeSpecial(0);
      Serial.println("absent");
      break;
    case Type::FLASH_STRING:
    {
      size_t len = strlen_P((const char *)this->flash_str);
      char str[len];
      memcpy_P(str, (const char *)this->flash_str, len);
      w.writeString(str, len);
      Serial.println("flash");
      break;
    }
    case Type::RAM_STRING:
      w.writeString(this->ram_str);
      Serial.println("ram");
      break;
    case Type::NUM:
      w.writeInt((long)this->num);
      Serial.println("num");
      break;
  }
}
