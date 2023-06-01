exports = async function(changeEvent) {
    const fullDocument = changeEvent.fullDocument;
    const oldDocument = changeEvent.fullDocumentBeforeChange;
    
    if (fullDocument && oldDocument) {
      const quant = fullDocument.stock.quantity;
      const maxStock = fullDocument.stock.max_stock_quantity;
      const oldQuant = oldDocument.stock.quantity;
    
      if (maxStock < quant) {
        const stockCollection = context.services
          .get("Cluster0")
          .db("Store")
          .collection("Product");
          
        const updatedStock = await stockCollection.updateOne(
          { _id: fullDocument._id },
          { $set: { "stock.quantity": oldQuant } }
        );
        
        console.log("Maximum stock exceeded. Reverted to the old value!");
      } else {
        console.log("Stock updated successfully!");
      }
    }
  };
  